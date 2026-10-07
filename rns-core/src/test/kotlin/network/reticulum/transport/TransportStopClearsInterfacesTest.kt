package network.reticulum.transport

import network.reticulum.common.InterfaceMode
import network.reticulum.common.RnsConstants
import network.reticulum.identity.Identity
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

/**
 * A stopped Reticulum must not leave detached interfaces registered. Every
 * other table stop() owns is cleared; this one was not, so a start/stop/start
 * cycle re-registered fresh interfaces alongside the dead ones and the path
 * table — reloaded from storage naming the previous session's interface hash
 * — then matched a detached interface. The first packet of the new session
 * went out on an interface that was not online and never retried.
 */
@DisplayName("Transport.stop clears the interface registry")
class TransportStopClearsInterfacesTest {

    private class StubInterface(
        override val name: String,
    ) : InterfaceRef {
        override val hash: ByteArray = ByteArray(RnsConstants.TRUNCATED_HASH_BYTES) { 0x11.toByte() }
        override val canSend: Boolean = true
        override val canReceive: Boolean = true
        override val online: Boolean = true
        override val mode: InterfaceMode = InterfaceMode.FULL
        override val hwMtu: Int = RnsConstants.MTU
        override var tunnelId: ByteArray? = null
        override var wantsTunnel: Boolean = false

        override fun send(data: ByteArray) {}
    }

    @Test
    fun `stop drops every registered interface`() {
        try {
            Transport.stop()
        } catch (_: Exception) {
        }
        Transport.start(Identity.create(), enableTransport = false)

        Transport.registerInterface(StubInterface(name = "stop-clears-${System.nanoTime()}"))
        assertEquals(1, Transport.getInterfaces().size)

        Transport.stop()
        assertEquals(0, Transport.getInterfaces().size)
    }
}
