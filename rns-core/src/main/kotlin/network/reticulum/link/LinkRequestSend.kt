package network.reticulum.link

/**
 * What to report after handing a link request to [network.reticulum.transport.Transport.outbound].
 *
 * `Transport.outbound` returns whether any interface accepted the packet, and
 * the link-request path used to discard that answer and log "sent" either way
 * (#591). A link request is not retried, so a refused transmit means the link
 * can only ever time out — and the device log read:
 *
 * ```
 * [Sideband] processOutgoing called but interface not online (online=false, detached=true)
 * [Transport] Transmit error on Sideband: Interface is not online
 * [Link] Link request 4d3e... sent to 38fea30e...
 * ```
 *
 * Three lines, the last one contradicting the first two, then thirty seconds
 * of silence. The information existed; it just never reached the line that
 * reported the outcome.
 *
 * Kept as a pure function so the honesty of the message is testable without a
 * transport, matching [network.reticulum.transport.OutboundRoute] and
 * `AnnounceFilter`.
 *
 * @return the failure text when [sent] is false, or null when the request went out.
 */
internal fun linkRequestSendFailure(
    sent: Boolean,
    linkId: String,
    destination: String,
): String? {
    if (sent) return null
    return "Link request $linkId to $destination was NOT sent: no interface " +
        "accepted it. A link request is not retried, so this link cannot be " +
        "established and will time out."
}
