package network.reticulum.link

import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * #591: the link-request path logged "sent" whether or not any interface took
 * the packet. These pin the two things that were wrong — that a refusal is
 * reported at all, and that the refusal never claims the request was sent.
 */
class LinkRequestSendTest {
    private val linkId = "4d3ea1b2c3d4e5f6"
    private val destination = "38fea30e2ae37493"

    @Test
    fun `a request that went out reports nothing`() {
        assertNull(linkRequestSendFailure(sent = true, linkId = linkId, destination = destination))
    }

    @Test
    fun `a refused request reports a failure`() {
        assertNotNull(
            linkRequestSendFailure(sent = false, linkId = linkId, destination = destination),
            "a transmit no interface accepted must be reported, not swallowed",
        )
    }

    @Test
    fun `the failure text never claims the request was sent`() {
        // The whole bug: "Link request <id> sent to <dest>" printed directly
        // after the interface and Transport had both logged the failure.
        val failure = linkRequestSendFailure(false, linkId, destination)!!
        assertTrue(
            !failure.contains(" sent to "),
            "the failure must not read like the success line it replaces: $failure",
        )
        assertTrue(failure.contains("NOT sent"), "the failure must say it was not sent: $failure")
    }

    @Test
    fun `the failure names the link and the destination`() {
        val failure = linkRequestSendFailure(false, linkId, destination)!!
        assertTrue(failure.contains(linkId), "must name the link: $failure")
        assertTrue(failure.contains(destination), "must name the destination: $failure")
    }

    @Test
    fun `the failure says the link will not recover on its own`() {
        // A link request is never retried. A reader who does not know that
        // sees a transmit error and waits, so the message has to say it.
        val failure = linkRequestSendFailure(false, linkId, destination)!!
        assertTrue(
            failure.contains("not retried") && failure.contains("time out"),
            "must say the link is not retried and will time out: $failure",
        )
    }
}
