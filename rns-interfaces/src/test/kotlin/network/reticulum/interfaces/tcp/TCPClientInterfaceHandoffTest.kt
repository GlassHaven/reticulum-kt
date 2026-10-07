package network.reticulum.interfaces.tcp

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.delay
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Timeout
import java.net.ServerSocket
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Tests for reconnect ownership handoff in TCPClientInterface.
 *
 * Covers the race where a reconnect request arrives while a reconnect
 * owner is still active: the request must be preserved (PENDING) and
 * honored once the owner releases, instead of being silently dropped.
 */
class TCPClientInterfaceHandoffTest {

    private val interfaces = mutableListOf<TCPClientInterface>()
    private val servers = mutableListOf<ServerSocket>()
    private val threads = mutableListOf<Thread>()

    @AfterEach
    fun cleanup() {
        interfaces.forEach { it.stop() }
        interfaces.clear()
        servers.forEach { runCatching { it.close() } }
        servers.clear()
        threads.forEach { it.interrupt() }
        threads.clear()
    }

    @Test
    @Timeout(20, unit = TimeUnit.SECONDS)
    fun `reconnect requested while an owner is active is honored after release`() {
        val server = ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress())
        servers.add(server)

        // Accept connections continuously and hold them open
        val accepted = CopyOnWriteArrayList<java.net.Socket>()
        val acceptThread = Thread {
            try {
                while (!server.isClosed) {
                    accepted.add(server.accept())
                }
            } catch (_: Exception) {
            }
        }
        acceptThread.isDaemon = true
        acceptThread.start()
        threads.add(acceptThread)

        val iface = TCPClientInterface(
            name = "TestHandoff",
            targetHost = "127.0.0.1",
            targetPort = server.localPort,
            connectTimeoutMs = 500,
        )
        interfaces.add(iface)

        val publishedConnections = AtomicInteger(0)
        iface.onConnectionPublishedForTest = {
            val count = publishedConnections.incrementAndGet()
            if (count == 1) {
                // Drop the live connection so the read loop EOFs and a
                // reconnect cycle starts
                runCatching { accepted.firstOrNull()?.close() }
            }
            if (count == 2) {
                // Model an outgoing failure after the replacement socket is
                // published but before reconnect() installs its read loop.
                // This requests reconnect while the owner is still active.
                iface.teardown()
            }
        }

        iface.start()

        runBlocking {
            // Connection 1: initial. Connection 2: first reconnect. The
            // teardown fired at publish 2 must produce connection 3.
            val deadline = System.currentTimeMillis() + 15_000
            while (publishedConnections.get() < 3 && System.currentTimeMillis() < deadline) {
                delay(100)
            }
        }

        assertEquals(
            3, publishedConnections.get(),
            "teardown-triggered reconnect during an active owner was dropped"
        )
        assertTrue(iface.online.value, "interface should be online after the honored reconnect")
    }
}