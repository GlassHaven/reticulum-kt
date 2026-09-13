package network.reticulum.interfaces.tcp

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import network.reticulum.identity.Identity
import network.reticulum.interfaces.IfacCredentials
import network.reticulum.interfaces.IfacUtils
import network.reticulum.interfaces.Interface
import network.reticulum.interfaces.backoff.ExponentialBackoff
import network.reticulum.interfaces.framing.HDLC
import network.reticulum.interfaces.framing.KISS
import java.io.IOException
import java.io.InputStream
import java.net.InetSocketAddress
import java.net.Socket
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
 * TCP client interface for Reticulum.
 *
 * Connects to a remote TCP server and exchanges packets using
 * HDLC or KISS framing.
 *
 * Debug logging can be enabled via system property: -Dreticulum.tcp.debug=true
 */
class TCPClientInterface(
    name: String,
    private val targetHost: String,
    private val targetPort: Int,
    private val useKissFraming: Boolean = false,
    private val connectTimeoutMs: Int = INITIAL_CONNECT_TIMEOUT,
    private val maxReconnectAttempts: Int? = null,
    /** Enable TCP keep-alive. Default true for Python RNS compatibility (can disable for mobile battery). */
    private val keepAlive: Boolean = true,
    // IFAC (Interface Access Code) parameters for network isolation
    override val ifacNetname: String? = null,
    override val ifacNetkey: String? = null,
    /**
     * Parent coroutine scope for lifecycle-aware cancellation.
     * When null (default), creates standalone scope - for JVM/standalone usage.
     * When provided, creates child scope that cancels when parent cancels - for Android service usage.
     */
    private val parentScope: CoroutineScope? = null,
    /**
     * Optional dialer used in place of the default `Socket()` + `connect()`
     * pair. Lets callers route the underlying TCP through a userspace
     * tunnel (WireGuard / Tailscale) or a SOCKS5/HTTP proxy without this
     * class needing to know about either. Receives `(host, port,
     * timeoutMs)` and must return a connected `java.net.Socket`. When
     * null (default) the interface dials directly through the kernel.
     *
     * Socket-level options (`tcpNoDelay`, `keepAlive`, `soTimeout`,
     * `soLinger`) are still applied after dial; userspace-tunnel sockets
     * generally accept-and-ignore them.
     */
    private val socketDialer: ((String, Int, Int) -> java.net.Socket)? = null,
) : Interface(name) {

    companion object {
        const val BITRATE_GUESS = 10_000_000 // 10 Mbps
        const val HW_MTU = 262144
        const val INITIAL_CONNECT_TIMEOUT = 5000 // 5 seconds

        @Deprecated("Use ExponentialBackoff instead", level = DeprecationLevel.WARNING)
        const val RECONNECT_WAIT_MS = 5000L // 5 seconds

        /** Enable verbose debug logging via -Dreticulum.tcp.debug=true */
        private val DEBUG = System.getProperty("reticulum.tcp.debug", "false").toBoolean()
    }

    override val bitrate: Int = BITRATE_GUESS
    override val hwMtu: Int = HW_MTU
    override val supportsLinkMtuDiscovery: Boolean = true

    // Discovery support
    override val supportsDiscovery: Boolean = true
    override val discoveryInterfaceType: String = "TCPClientInterface"
    override fun getDiscoveryData(): Map<Int, Any> = mapOf(
        network.reticulum.discovery.DiscoveryConstants.REACHABLE_ON to targetHost,
        network.reticulum.discovery.DiscoveryConstants.PORT to targetPort,
    )

    // IFAC credentials - derived lazily from network name/passphrase
    private val _ifacCredentials: IfacCredentials? by lazy {
        IfacUtils.deriveIfacCredentials(ifacNetname, ifacNetkey)
    }

    override val ifacSize: Int
        get() = if (_ifacCredentials != null) 16 else 0

    override val ifacKey: ByteArray?
        get() = _ifacCredentials?.key

    override val ifacIdentity: Identity?
        get() = _ifacCredentials?.identity

    private enum class ReconnectState {
        IDLE,
        RUNNING,
        PENDING,
    }

    private var socket: Socket? = null
    private val reconnectState = AtomicReference(ReconnectState.IDLE)
    private val neverConnected = AtomicBoolean(true)
    private val writing = AtomicBoolean(false)

    // Debug counters
    private val framesSent = AtomicLong(0)
    private val framesReceived = AtomicLong(0)

    // Exponential backoff for reconnection: 1s, 2s, 4s... up to 60s, give up after maxReconnectAttempts
    private val backoff = ExponentialBackoff(
        maxAttempts = maxReconnectAttempts ?: 10
    )

    // Coroutine scope for I/O operations (battery-efficient on Android)
    private val ioScope: CoroutineScope = createScope(parentScope).also {
        // Listen for parent cancellation AFTER scope is created (avoids race condition)
        // When parent scope completes (cancelled or otherwise), trigger graceful shutdown
        parentScope?.coroutineContext?.get(Job)?.invokeOnCompletion { _ ->
            // Parent completed - trigger graceful shutdown
            // Note: This fires after parent starts cancelling AND its children complete,
            // so also monitor the child scope's cancellation state
            detach()
        }
        // Additionally, launch a coroutine that watches for scope cancellation
        // This provides faster response to parent cancellation
        parentScope?.launch {
            try {
                // This coroutine will be cancelled when parent is cancelled
                kotlinx.coroutines.awaitCancellation()
            } finally {
                // Parent scope was cancelled - trigger shutdown
                detach()
            }
        }
    }
    private var readJob: Job? = null
    private var connectJob: Job? = null

    /** Test hook: invoked after a socket has been published as online. */
    internal var onConnectionPublishedForTest: (() -> Unit)? = null

    private data class EstablishedConnection(
        val socket: Socket,
        val inputStream: InputStream,
    )

    /**
     * Create the appropriate coroutine scope based on parent.
     * - With parent: child scope that cancels when parent cancels (Android service lifecycle)
     * - Without parent: standalone scope for JVM/test usage
     */
    private fun createScope(parent: CoroutineScope?): CoroutineScope {
        return if (parent != null) {
            // Child scope: cancels when parent cancels, but can cancel independently
            // SupervisorJob(parentJob) creates a child that doesn't propagate failures upward
            CoroutineScope(parent.coroutineContext + SupervisorJob(parent.coroutineContext[Job]) + Dispatchers.IO)
        } else {
            // Standalone scope: lives until explicitly cancelled
            CoroutineScope(SupervisorJob() + Dispatchers.IO)
        }
    }

    private val hdlcDeframer = HDLC.createDeframer { data ->
        val frameNum = framesReceived.incrementAndGet()
        if (DEBUG) {
            val hexPreview = data.take(16).joinToString(" ") { "%02x".format(it) }
            val suffix = if (data.size > 16) "..." else ""
            debugLog("RECV frame #$frameNum: ${data.size} bytes, data=[$hexPreview$suffix]")
        }
        processIncoming(data)
    }

    private val kissDeframer = KISS.createDeframer { _, data ->
        val frameNum = framesReceived.incrementAndGet()
        if (DEBUG) {
            val hexPreview = data.take(16).joinToString(" ") { "%02x".format(it) }
            val suffix = if (data.size > 16) "..." else ""
            debugLog("RECV frame #$frameNum (KISS): ${data.size} bytes, data=[$hexPreview$suffix]")
        }
        processIncoming(data)
    }

    override fun start() {
        connectJob = ioScope.launch {
            val connection = connect(initial = true)
            if (connection == null) {
                reconnect()
            } else {
                startReadLoop(connection.socket, connection.inputStream)
            }
        }
    }

    private fun connect(initial: Boolean = false): EstablishedConnection? {
        return try {
            if (initial) {
                log("Establishing TCP connection to $targetHost:$targetPort...")
            }

            val sock = socketDialer?.invoke(targetHost, targetPort, connectTimeoutMs)
                ?: Socket().also {
                    it.connect(InetSocketAddress(targetHost, targetPort), connectTimeoutMs)
                }
            sock.tcpNoDelay = true
            sock.keepAlive = keepAlive
            sock.soTimeout = 0 // Block on read
            sock.setSoLinger(true, 5) // Clean shutdown with 5s linger (prevents RST on close)

            // Get input stream immediately while we have the socket reference
            val inputStream = sock.getInputStream()

            socket = sock
            online.set(true)
            neverConnected.set(false)
            onConnectionPublishedForTest?.invoke()

            // Request tunnel synthesis for this connection
            wantsTunnel = true

            if (initial) {
                log("TCP connection established")
            }

            // Debug: Log socket state after connection
            if (DEBUG) {
                debugLog("Socket connected - state dump:")
                debugLog("  localAddress: ${sock.localSocketAddress}")
                debugLog("  remoteAddress: ${sock.remoteSocketAddress}")
                debugLog("  tcpNoDelay: ${sock.tcpNoDelay}")
                debugLog("  keepAlive: ${sock.keepAlive}")
                debugLog("  soTimeout: ${sock.soTimeout}")
                debugLog("  receiveBufferSize: ${sock.receiveBufferSize}")
                debugLog("  sendBufferSize: ${sock.sendBufferSize}")
                debugLog("  soLinger: ${sock.soLinger}")
                debugLog("  reuseAddress: ${sock.reuseAddress}")
            }

            // Send an empty HDLC frame to "activate" the connection
            // This triggers Python RNS's read_loop recv() to return, which ensures
            // the handler thread is fully blocked and won't close the socket
            try {
                val keepaliveFrame = HDLC.frame(ByteArray(0))
                sock.getOutputStream().write(keepaliveFrame)
                sock.getOutputStream().flush()
            } catch (e: Exception) {
                // Keepalive failure is not critical
            }

            // Delay for Python's ThreadingMixIn to spawn handler and enter read_loop()
            // 100ms gives time for the handler thread to start blocking on recv()
            Thread.sleep(100)

            // Pass socket and stream directly to avoid race conditions; the
            // caller installs the read loop only after reconnect ownership
            // has been released
            EstablishedConnection(sock, inputStream)
        } catch (e: Exception) {
            if (initial) {
                log("Initial connection failed: ${e.message}")
                log("Will retry with exponential backoff (1s, 2s, 4s... up to 60s)")
            }
            null
        }
    }

    private suspend fun reconnect() {
        if (!claimReconnectOwnership()) return

        // Note: Do NOT reset backoff here - network change handler will reset when appropriate
        // This allows progressive backoff across reconnect cycles

        var connection: EstablishedConnection? = null
        var reconnectWasPending = false
        try {
            while (!online.get() && !detached.get()) {
                val delayMs = backoff.nextDelay()

                if (delayMs == null) {
                    log("Max reconnection attempts (${backoff.attemptCount}) reached, giving up")
                    detach()
                    break
                }

                delay(delayMs)

                // Check if scope still active after delay
                if (!ioScope.isActive) break

                try {
                    connection = connect()
                    if (connection != null) {
                        if (!neverConnected.get()) {
                            log("Reconnected successfully after ${backoff.attemptCount} attempts")
                        }
                        backoff.reset() // Success - reset for next time
                        break
                    }
                } catch (e: CancellationException) {
                    // Scope was cancelled, stop reconnecting
                    break
                } catch (e: Exception) {
                    log("Reconnection attempt ${backoff.attemptCount} failed: ${e.message}")
                }
            }
        } catch (e: CancellationException) {
            // Scope was cancelled, stop reconnecting
        } finally {
            // Clear reconnect ownership before starting the replacement read
            // loop. This ensures an immediately closed replacement socket can
            // synchronously claim the next reconnect instead of being dropped.
            reconnectWasPending = releaseReconnectOwnership()
        }

        // A write failure can tear down a newly published socket before its
        // reader is installed. If that teardown requested reconnect while this
        // owner was active, take responsibility for the deferred request now.
        if (
            reconnectWasPending &&
            ioScope.isActive &&
            !online.get() &&
            !detached.get()
        ) {
            ioScope.launch { reconnect() }
        }
    }

    private fun claimReconnectOwnership(): Boolean {
        while (true) {
            when (reconnectState.get()) {
                ReconnectState.IDLE -> {
                    if (reconnectState.compareAndSet(ReconnectState.IDLE, ReconnectState.RUNNING)) {
                        return true
                    }
                }

                ReconnectState.RUNNING -> {
                    if (reconnectState.compareAndSet(ReconnectState.RUNNING, ReconnectState.PENDING)) {
                        return false
                    }
                }

                ReconnectState.PENDING -> return false
            }
        }
    }

    private fun releaseReconnectOwnership(): Boolean {
        while (true) {
            when (reconnectState.get()) {
                ReconnectState.RUNNING -> {
                    if (reconnectState.compareAndSet(ReconnectState.RUNNING, ReconnectState.IDLE)) {
                        return false
                    }
                }

                ReconnectState.PENDING -> {
                    if (reconnectState.compareAndSet(ReconnectState.PENDING, ReconnectState.IDLE)) {
                        return true
                    }
                }

                ReconnectState.IDLE -> return false
            }
        }
    }

    private fun startReadLoop(sock: Socket, inputStream: InputStream) {
        readJob = ioScope.launch {
            val buffer = ByteArray(4096)

            try {
                while (isActive && online.get() && !detached.get()) {
                    if (sock.isClosed || !sock.isConnected || sock.isInputShutdown) {
                        break
                    }

                    // Blocking read wrapped in IO dispatcher
                    val bytesRead = withContext(Dispatchers.IO) {
                        inputStream.read(buffer)
                    }

                    if (bytesRead > 0) {
                        val data = buffer.copyOf(bytesRead)
                        if (useKissFraming) {
                            kissDeframer.process(data)
                        } else {
                            hdlcDeframer.process(data)
                        }
                    } else if (bytesRead == -1) {
                        // Connection closed
                        if (DEBUG) {
                            debugLog("Read loop: EOF received (bytesRead=-1), connection closed by peer")
                            debugLog("  frames sent: ${framesSent.get()}, frames received: ${framesReceived.get()}")
                            debugLog("  socket state: isConnected=${sock.isConnected}, isClosed=${sock.isClosed}")
                            debugLog("  socket state: isInputShutdown=${sock.isInputShutdown}, isOutputShutdown=${sock.isOutputShutdown}")
                        }
                        online.set(false)
                        break
                    }
                }
            } catch (e: CancellationException) {
                // Normal cancellation, don't log as error
                if (DEBUG) {
                    debugLog("Read loop: CancellationException (normal shutdown)")
                }
            } catch (e: IOException) {
                if (DEBUG) {
                    debugLog("Read loop: IOException - ${e.javaClass.name}: ${e.message}")
                    debugLog("  frames sent: ${framesSent.get()}, frames received: ${framesReceived.get()}")
                    try {
                        debugLog("  socket state: isConnected=${sock.isConnected}, isClosed=${sock.isClosed}")
                        debugLog("  socket state: isInputShutdown=${sock.isInputShutdown}, isOutputShutdown=${sock.isOutputShutdown}")
                    } catch (ex: Exception) {
                        debugLog("  socket state: unavailable (${ex.message})")
                    }
                }
                if (!detached.get()) {
                    online.set(false)
                }
            }

            // Connection lost, try to reconnect
            if (!detached.get() && isActive) {
                log("Connection lost, attempting to reconnect...")
                reconnect()
            }
        }
    }

    override fun processOutgoing(data: ByteArray) {
        if (!online.get() || detached.get()) {
            throw IllegalStateException("Interface is not online")
        }

        val sock = socket ?: throw IllegalStateException("Socket is null")

        // Verify connection state before attempting write
        if (!sock.isConnected || sock.isClosed || sock.isOutputShutdown) {
            val state = "isConnected=${sock.isConnected}, isClosed=${sock.isClosed}, isOutputShutdown=${sock.isOutputShutdown}"
            log("Socket not in valid state for write: $state")
            teardown()
            throw IOException("Socket not in valid state for write: $state")
        }

        // Wait for any pending write to complete
        while (writing.get()) {
            Thread.sleep(10)
        }

        try {
            writing.set(true)

            val framedData = if (useKissFraming) {
                KISS.frame(data)
            } else {
                HDLC.frame(data)
            }

            // Get output stream once for atomic write+flush
            val outputStream = sock.getOutputStream()

            // Write and flush atomically - if either fails, teardown
            try {
                outputStream.write(framedData)
                outputStream.flush()
            } catch (e: java.net.SocketTimeoutException) {
                // Specific timeout handling with diagnostic info
                log("Write timeout: ${e.message}")
                if (DEBUG) {
                    debugLog("SocketTimeoutException during write:")
                    debugLog("  data size: ${framedData.size} bytes")
                    debugLog("  socket state: isConnected=${sock.isConnected}, isClosed=${sock.isClosed}")
                }
                teardown()
                throw e
            }

            val frameNum = framesSent.incrementAndGet()
            if (DEBUG) {
                val hexPreview = data.take(16).joinToString(" ") { "%02x".format(it) }
                val suffix = if (data.size > 16) "..." else ""
                debugLog("SENT frame #$frameNum: ${data.size} bytes (framed: ${framedData.size}), data=[$hexPreview$suffix]")
                debugLog("  socket.isConnected: ${sock.isConnected}, socket.isClosed: ${sock.isClosed}")
            }

            txBytes.addAndGet(framedData.size.toLong())
            parentInterface?.txBytes?.addAndGet(framedData.size.toLong())

        } catch (e: IOException) {
            log("Write error: ${e.message}")
            if (DEBUG) {
                debugLog("Write IOException - full details:")
                debugLog("  exception: ${e.javaClass.name}: ${e.message}")
                debugLog("  socket state: isConnected=${sock.isConnected}, isClosed=${sock.isClosed}")
                debugLog("  socket state: isInputShutdown=${sock.isInputShutdown}, isOutputShutdown=${sock.isOutputShutdown}")
                debugLog("  frames sent before error: ${framesSent.get()}")
                debugLog("  frames received before error: ${framesReceived.get()}")
            }
            teardown()
            throw e
        } finally {
            writing.set(false)
        }
    }

    /**
     * Stop the interface gracefully.
     * Cancels all coroutines and closes the socket.
     * This is the explicit cleanup path - called directly in JVM usage
     * or triggered automatically when parent scope is cancelled.
     */
    fun stop() {
        detach()
    }

    /**
     * Notify the interface that the network has changed.
     *
     * This resets the reconnection backoff counter, allowing quick
     * reconnection attempts on the new network. Call this when:
     * - WiFi <-> cellular handoff occurs
     * - Network becomes available after being offline
     *
     * Per CONTEXT.md: "Network changes reset the backoff counter"
     */
    fun onNetworkChanged() {
        log("Network changed - resetting reconnection backoff")
        backoff.reset()

        // If currently offline and not detached, trigger reconnection
        if (!online.get() && !detached.get() && reconnectState.get() == ReconnectState.IDLE) {
            ioScope.launch {
                reconnect()
            }
        }
    }

    override fun detach() {
        super.detach()
        // Cancel all coroutines first
        readJob?.cancel()
        connectJob?.cancel()
        ioScope.cancel()
        closeSocket()
    }

    internal fun teardown() {
        if (DEBUG) {
            debugLog("Teardown called - transitioning to OFFLINE")
            debugLog("  frames sent: ${framesSent.get()}, frames received: ${framesReceived.get()}")
            debugLog("  detached: ${detached.get()}")
        }
        online.set(false)
        closeSocket()

        if (!detached.get()) {
            ioScope.launch {
                reconnect()
            }
        }
    }

    private fun closeSocket() {
        try {
            socket?.close()
        } catch (e: Exception) {
            // Ignore
        }
        socket = null
    }

    private fun log(message: String) {
        val timestamp = java.time.LocalDateTime.now().format(
            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
        )
        println("[$timestamp] [$name] $message")
    }

    private fun debugLog(message: String) {
        if (DEBUG) {
            val timestamp = java.time.LocalDateTime.now().format(
                java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
            )
            println("[$timestamp] [$name] [DEBUG] $message")
        }
    }

    override fun toString(): String = "TCPClientInterface[$name -> $targetHost:$targetPort]"
}
