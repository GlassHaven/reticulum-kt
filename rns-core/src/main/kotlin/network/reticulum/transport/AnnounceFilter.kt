package network.reticulum.transport

import network.reticulum.common.InterfaceMode

/**
 * Determines whether an announce should be forwarded to a given interface,
 * based on the Python reference implementation (Transport.py:1040-1084).
 */
object AnnounceFilter {

    /**
     * Check whether an announce should be forwarded to an interface with
     * [outgoingMode].
     *
     * @param outgoingMode The mode of the interface we'd send the announce on
     * @param isLocalDestination Whether the destination is hosted on this instance
     * @param sourceMode The mode of the next-hop interface where the announce
     *   was originally received (null if unknown)
     * @param outgoingAnnouncesFromInternal Whether the outgoing interface
     *   re-broadcasts announces from an INTERNAL next-hop
     *   (`announces_from_internal`, Python `Interface.announces_from_internal`,
     *   default true, Interface.py:123). When false, Gate 1 blocks the
     *   announce if the next hop is INTERNAL (Transport.py:1471).
     * @return true if the announce should be forwarded
     */
    fun shouldForward(
        outgoingMode: InterfaceMode,
        isLocalDestination: Boolean,
        sourceMode: InterfaceMode?,
        outgoingAnnouncesFromInternal: Boolean = true,
    ): Boolean {
        // Gate 1 (Transport.py:1471): an outgoing interface with
        // announces_from_internal=False blocks an announce whose next hop is
        // INTERNAL. Python checks this BEFORE the outgoing-mode switch, so it
        // fires for ANY outgoing mode (not just FULL/POINT_TO_POINT/GATEWAY).
        // Local destinations are exempt. `sourceMode == INTERNAL` implies the
        // next-hop interface exists (mode is non-null), so this does not fire
        // for the "next hop doesn't exist" (null) case that the mode branches
        // below block separately.
        if (!isLocalDestination && sourceMode == InterfaceMode.INTERNAL && !outgoingAnnouncesFromInternal) {
            return false
        }

        return when (outgoingMode) {
            // AP mode: never broadcast announces
            InterfaceMode.ACCESS_POINT -> false

            // INTERNAL mode (RNS 1.3.6+): allow by default; block only when
            // the next hop has no mode (null) or is BOUNDARY
            // (Transport.py:1479-1490). `announces_to_internal` is a no-op
            // in the 1.5 gate (True and not-True both fall through to allow).
            InterfaceMode.INTERNAL -> {
                if (isLocalDestination) true
                else when (sourceMode) {
                    null, InterfaceMode.BOUNDARY -> false
                    else -> true
                }
            }

            // ROAMING mode: allow local destinations; block if source is ROAMING or BOUNDARY
            InterfaceMode.ROAMING -> {
                if (isLocalDestination) true
                else when (sourceMode) {
                    InterfaceMode.ROAMING, InterfaceMode.BOUNDARY -> false
                    null -> false
                    else -> true
                }
            }

            // BOUNDARY mode: allow local destinations; block if source is ROAMING
            InterfaceMode.BOUNDARY -> {
                if (isLocalDestination) true
                else when (sourceMode) {
                    InterfaceMode.ROAMING -> false
                    null -> false
                    else -> true
                }
            }

            // FULL, POINT_TO_POINT, GATEWAY: no mode-based restrictions
            else -> true
        }
    }

    /**
     * Get the path expiry duration for a given interface mode.
     * Matches Python Transport.py:1730-1735.
     */
    fun pathExpiryForMode(mode: InterfaceMode): Long {
        return when (mode) {
            InterfaceMode.ACCESS_POINT -> TransportConstants.AP_PATH_TIME
            InterfaceMode.ROAMING -> TransportConstants.ROAMING_PATH_TIME
            else -> TransportConstants.PATHFINDER_E
        }
    }
}
