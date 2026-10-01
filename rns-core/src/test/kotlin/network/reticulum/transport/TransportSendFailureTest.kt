package network.reticulum.transport

import io.kotest.matchers.shouldBe
import network.reticulum.common.DestinationType
import network.reticulum.common.HeaderType
import network.reticulum.common.InterfaceMode
import network.reticulum.common.PacketContext
import network.reticulum.common.PacketType
import network.reticulum.common.RnsConstants
import network.reticulum.common.toKey
import network.reticulum.identity.Identity
import network.reticulum.packet.Packet
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

/**
 * Regression guard for the PR #89 review finding: [Transport.transmit] used
 * to swallow a throwing [InterfaceRef.send] and [Transport.processOutbound]
 * still marked the packet as sent. Callers such as [network.reticulum.link.Link]
 * reported "Link request sent" and left [network.reticulum.link.Link.establishmentError]
 * null even though nothing left the device — the classic case is an interface
 * that detaches while still reporting online.
 *
 * transmit() now returns true only when send() completed, and every
 * processOutbound branch that records `sent` takes that result. A throw on
 * one interface of many must not zero out a real send on another, so the
 * broadcast case asserts partial success still counts.
 *
 * The slow E2E equivalent lives in the reticulum-conformance wire tests.
 */
@DisplayName("Transport.outbound() must not report success when the interface send throws")
class TransportSendFailureTest {

    /** Records bytes handed to [send]; the working-interface fixture. */
    private class CapturingInterface(
        override val name: String,
    ) : InterfaceRef {
        val sent = mutableListOf<ByteArray>()

        override val hash: ByteArray = ByteArray(RnsConstants.TRUNCATED_HASH_BYTES) { 0xAA.toByte() }
        override val canSend: Boolean = true
        override val canReceive: Boolean = true
        override val online: Boolean = true
        override val mode: InterfaceMode = InterfaceMode.FULL
        override val bitrate: Int = 1_000_000
        override val hwMtu: Int = RnsConstants.MTU

        override var tunnelId: ByteArray? = null
        override var wantsTunnel: Boolean = false

        override fun send(data: ByteArray) {
            sent.add(data.copyOf())
        }
    }

    /** Every [send] throws — the detached-but-still-online failure mode. */
    private class ThrowingInterface(
        override val name: String,
    ) : InterfaceRef {
        val attempts = mutableListOf<ByteArray>()

        override val hash: ByteArray = ByteArray(RnsConstants.TRUNCATED_HASH_BYTES) { 0xAB.toByte() }
        override val canSend: Boolean = true
        override val canReceive: Boolean = true
        override val online: Boolean = true
        override val mode: InterfaceMode = InterfaceMode.FULL
        override val bitrate: Int = 1_000_000
        override val hwMtu: Int = RnsConstants.MTU

        override var tunnelId: ByteArray? = null
        override var wantsTunnel: Boolean = false

        override fun send(data: ByteArray) {
            attempts.add(data.copyOf())
            throw IllegalStateException("interface detached")
        }
    }

    private val interfaces = mutableListOf<InterfaceRef>()

    @BeforeEach
    fun setup() {
        try {
            Transport.stop()
        } catch (_: Exception) {
            // Best-effort — a prior test may have left things in an odd state.
        }
        Transport.pathTable.clear()
        Transport.start(Identity.create(), enableTransport = false)
    }

    @AfterEach
    fun teardown() {
        for (iface in interfaces) {
            try {
                Transport.deregisterInterface(iface)
            } catch (_: Exception) {
                // Best-effort.
            }
        }
        interfaces.clear()
        Transport.pathTable.clear()
        try {
            Transport.stop()
        } catch (_: Exception) {
            // Best-effort.
        }
    }

    private fun register(vararg ifs: InterfaceRef) {
        for (iface in ifs) {
            Transport.registerInterface(iface)
            interfaces.add(iface)
        }
    }

    private fun installOneHopPath(destHash: ByteArray, iface: InterfaceRef) {
        val now = System.currentTimeMillis()
        val entry = PathEntry(
            timestamp = now,
            nextHop = ByteArray(RnsConstants.TRUNCATED_HASH_BYTES) { 0xBB.toByte() },
            hops = 1,
            expires = now + TransportConstants.PATHFINDER_E,
            randomBlobs = mutableListOf(),
            receivingInterfaceHash = iface.hash,
            announcePacketHash = ByteArray(RnsConstants.TRUNCATED_HASH_BYTES) { 0xCC.toByte() },
            state = PathState.ACTIVE,
            failureCount = 0,
        )
        Transport.pathTable[destHash.toKey()] = entry
    }

    private fun dataPacket(destHash: ByteArray): Packet =
        Packet.createRaw(
            destinationHash = destHash,
            data = ByteArray(32) { 0x11.toByte() },
            packetType = PacketType.DATA,
            destinationType = DestinationType.SINGLE,
            context = PacketContext.NONE,
            headerType = HeaderType.HEADER_1,
            createReceipt = false,
        )

    @Test
    @DisplayName("path-routed send that throws reports outbound=false")
    fun pathSendThrowReportsFailure() {
        val broken = ThrowingInterface(name = "broken-${System.nanoTime()}")
        register(broken)

        val destHash = ByteArray(RnsConstants.TRUNCATED_HASH_BYTES) { (it + 1).toByte() }
        installOneHopPath(destHash, broken)

        Transport.outbound(dataPacket(destHash)) shouldBe false

        // Two attempts, both failed: the path-routed send, then the
        // broadcast fallback that processOutbound runs when sent stayed
        // false — it retries on every online interface, which here is the
        // same broken one. outbound() is false because neither attempt
        // actually got the packet onto a working interface.
        broken.attempts.size shouldBe 2
    }

    @Test
    @DisplayName("attached-interface send that throws reports outbound=false")
    fun attachedInterfaceSendThrowReportsFailure() {
        val broken = ThrowingInterface(name = "broken-${System.nanoTime()}")
        register(broken)

        val destHash = ByteArray(RnsConstants.TRUNCATED_HASH_BYTES) { (it + 2).toByte() }
        val packet = dataPacket(destHash)
        packet.attachedInterface = broken

        Transport.outbound(packet) shouldBe false
        broken.attempts.size shouldBe 1
    }

    @Test
    @DisplayName("broadcast counts partial success: one throwing interface does not zero a real send")
    fun broadcastPartialSuccessStillSent() {
        val broken = ThrowingInterface(name = "broken-${System.nanoTime()}")
        val working = CapturingInterface(name = "working-${System.nanoTime()}")
        register(broken, working)

        // No path entry, no attached interface — falls through to broadcast
        // on every online canSend interface.
        val destHash = ByteArray(RnsConstants.TRUNCATED_HASH_BYTES) { (it + 3).toByte() }

        Transport.outbound(dataPacket(destHash)) shouldBe true
        broken.attempts.size shouldBe 1
        working.sent.size shouldBe 1
    }
}