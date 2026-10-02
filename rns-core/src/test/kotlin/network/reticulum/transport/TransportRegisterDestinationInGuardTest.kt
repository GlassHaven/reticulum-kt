package network.reticulum.transport

import network.reticulum.common.DestinationDirection
import network.reticulum.common.DestinationType
import network.reticulum.destination.Destination
import network.reticulum.identity.Identity
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

/**
 * `Transport.registerDestination` must only register IN-direction destinations.
 *
 * Python's `register_destination` (RNS/Transport.py:2898) appends to the
 * destination table only when `destination.direction == IN`. The kotlin port
 * kept the unconditional append, so a remote peer's OUT destination - registered
 * as a side effect of sending to it - landed in `Transport.destinations`.
 * Four readers then misbehaved: `isLocalDestination` (announce-skip at the
 * `Skipping announce for local destination` log line) treated a remote peer as
 * local and skipped its announces; `findDestination` could return a remote OUT
 * destination; and the mode-based announce-forward and path-response local
 * checks mis-classified remote peers. The table silently accumulated every peer
 * ever messaged.
 *
 * `sharedConnectionReappeared` (the only reader that already guarded on
 * `direction == IN` when iterating the same list) proved the list was expected
 * to be IN-only - the missing filter was an internal inconsistency.
 */
@DisplayName("Transport.registerDestination filters to IN destinations only")
class TransportRegisterDestinationInGuardTest {

    @Test
    fun `an OUT destination for a remote peer is not registered and its announce is not treated as local`() {
        try {
            Transport.stop()
        } catch (_: Exception) {
        }
        Transport.start(Identity.create(), enableTransport = false)

        // A remote peer we are sending to: an OUT destination for its identity.
        val remoteIdentity = Identity.create()
        val outDestination = Destination.create(
            identity = remoteIdentity,
            direction = DestinationDirection.OUT,
            type = DestinationType.SINGLE,
            appName = "test",
        )

        // The bug: registering this OUT destination (as the send path does) put
        // the remote peer into Transport.destinations. The guard must prevent
        // that, so the peer's announces are processed rather than skipped as a
        // "local destination".
        Transport.registerDestination(outDestination)

        assertNull(
            Transport.findDestination(outDestination.hash),
            "An OUT destination must not be registered; registering it would make " +
                "isLocalDestination treat the remote peer as local and skip its announces",
        )
        assertTrue(
            Transport.getDestinations().none { it.hash.contentEquals(outDestination.hash) },
            "Transport.destinations must not contain an OUT (remote) destination",
        )
    }

    @Test
    fun `an IN destination is still registered`() {
        try {
            Transport.stop()
        } catch (_: Exception) {
        }
        Transport.start(Identity.create(), enableTransport = false)

        val inDestination = Destination.create(
            identity = null,
            direction = DestinationDirection.IN,
            type = DestinationType.SINGLE,
            appName = "test",
        )

        // IN destinations are the ones that legitimately belong in the table:
        // they back our own announces and the local-destination announce-skip
        // that prevents 0-hop path-table loops from bounced announces.
        Transport.registerDestination(inDestination)

        assertNotNull(
            Transport.findDestination(inDestination.hash),
            "An IN destination must still be registered",
        )
    }
}
