package network.reticulum.resource

import network.reticulum.common.DestinationDirection
import network.reticulum.common.DestinationType
import network.reticulum.common.InterfaceMode
import network.reticulum.common.PacketContext
import network.reticulum.common.RnsConstants
import network.reticulum.destination.Destination
import network.reticulum.identity.Identity
import network.reticulum.link.Link
import network.reticulum.transport.InterfaceRef
import network.reticulum.transport.Transport
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

/**
 * Regression tests for the two concrete bugs documented in reticulum-kt#65.
 *
 * The perf investigation in that issue is OUT OF SCOPE here; these two tests
 * pin the two real bugs the perf investigation surfaced.
 *
 * Bug 2 (failed-callback registration race): Resource.create()/accept() spawn
 * the watchdog (create() via advertise(); accept() via startWatchdog()) before
 * the application can install callbacks.failed, so a failure that fires in that
 * window is invoked on null (?. no-op) and never propagated up to
 * LXMessage.state. Fixed by a failedCallback parameter that is installed on
 * callbacks BEFORE the watchdog can fire.
 *
 * Bug 1 (sender watchdog has no recovery actions): the pre-fix kotlin watchdog
 * only retried on the receiver side (`if (!initiator) requestNext()`); a sender
 * in ADVERTISED or AWAITING_PROOF simply counted to MAX_RETRIES and cancelled.
 * Python Resource.py:560-670 re-sends the advertisement on an ADVERTISED timeout
 * and queries the proof from the network cache (Transport.cache_request) on an
 * AWAITING_PROOF timeout, only cancelling on a TRANSFERRING timeout. These tests
 * drive the per-iteration watchdog check (watchdogTickForTest) synchronously on a
 * real, encryptable sender Resource, so the recovery actions are observable
 * without a live link:
 *  - ADVERTISED + timeout + retries left -> a fresh RESOURCE_ADV frame appears at
 *    the link's interface (the re-send), status stays ADVERTISED.
 *  - ADVERTISED + timeout + no retries  -> the transfer is cancelled (no re-send).
 *  - AWAITING_PROOF + timeout + retries left -> Transport.cache_request is invoked
 *    for the expected proof (observed via proofCacheQueriesForTest), status stays
 *    AWAITING_PROOF.
 *
 * The sender Resource is built over a fresh (never-ACTIVE) link whose linkId and
 * derivedKey are primed via reflection, mirroring the reference harness driving a
 * python Resource on a stubbed link. Resource has a private constructor and
 * private fields, so the link is primed the same way the existing integrity tests
 * build their Resources (see ResourceAssemblyIntegrityFailureTest).
 */
@DisplayName("Resource issue #65 bugs (watchdog recovery + failed-callback race)")
class ResourceIssue65BugTest {

    /** Records every raw frame the link layer emits. */
    private class CapturingInterface(override val name: String) : InterfaceRef {
        val frames = java.util.Collections.synchronizedList(mutableListOf<ByteArray>())

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
            frames.add(data.copyOf())
        }
    }

    private lateinit var iface: CapturingInterface

    /** HEADER_1 layout: [flags][hops][dest:16][context][body] -> context at 18. */
    private fun countFrames(iface: CapturingInterface, contextByte: Int): Int {
        synchronized(iface.frames) {
            var count = 0
            for (raw in iface.frames) {
                if (raw.size > 19 && (raw[18].toInt() and 0xFF) == contextByte) count++
            }
            return count
        }
    }

    // ===== reflection helpers (Resource and Link have private fields) =====

    private fun <T> set(target: Any, klass: Class<*>, name: String, value: T) {
        val f = klass.getDeclaredField(name)
        f.isAccessible = true
        f.set(target, value)
    }

    /**
     * Prime a fresh (never-ACTIVE) link so it is a usable SENDER endpoint:
     * a 16-byte linkId (createRaw's destination_hash requirement) and a 32-byte
     * derivedKey (so link.encrypt() has a Token key). A real initiator link only
     * gets these after a handshake; the reference harness tests the Resource
     * state machine against a stubbed link, which is exactly what this mirrors.
     */
    private fun primeWorkingLink(link: Link) {
        set(link, Link::class.java, "linkId", ByteArray(RnsConstants.TRUNCATED_HASH_BYTES) { (it + 1).toByte() })
        set(link, Link::class.java, "derivedKey", ByteArray(32) { (it * 7).toByte() })
    }

    /** Build a real SENDER Resource (encryptable, with parts/hash/expectedProof). */
    private fun senderResource(link: Link): Resource {
        primeWorkingLink(link)
        return Resource.create(
            data = ByteArray(4096) { (it % 251).toByte() },
            link = link,
            advertise = false, // suppress the auto watchdog; the test drives it
            autoCompress = false,
        )
    }

    private fun freshOutLink(appName: String, aspect: String): Link {
        val dest = Destination.create(
            identity = Identity.create(),
            direction = DestinationDirection.OUT,
            type = DestinationType.SINGLE,
            appName = appName,
            aspects = arrayOf(aspect),
        )
        return Link.create(dest)
    }

    /** Route the link's DATA traffic to the capturing interface. */
    private fun routeLinkToIface(link: Link) {
        Transport.registerLinkPath(link.linkId, iface.hash, 1)
    }

    @BeforeEach
    fun setup() {
        try {
            Transport.stop()
        } catch (_: Exception) {
            // Best-effort - a prior test may have left things in an odd state.
        }
        Transport.pathTable.clear()
        Transport.start(Identity.create(), enableTransport = false)
        iface = CapturingInterface(name = "issue65-${System.nanoTime()}")
        Transport.registerInterface(iface)
    }

    @AfterEach
    fun teardown() {
        Resource.watchdogDisabledForTest = false
        try {
            Transport.deregisterInterface(iface)
        } catch (_: Exception) {
            // Best-effort.
        }
        Transport.pathTable.clear()
        try {
            Transport.stop()
        } catch (_: Exception) {
            // Best-effort.
        }
    }

    // ===== Bug 2: failed-callback registration race =====

    @Test
    @DisplayName("create() installs the failed callback synchronously before the watchdog can fire")
    fun createInstallsFailedCallbackBeforeWatchdog() {
        val link = freshOutLink("issue65cb", "failedcb")
        primeWorkingLink(link)
        var failed = false

        // The failedCallback parameter is installed on callbacks BEFORE
        // advertise() could spawn the watchdog (issue #65). Pre-fix this
        // parameter did not exist (compile-fail red): the application set
        // callbacks.failed AFTER create() returned, leaving a window where the
        // watchdog could fire failed on null (?. no-op).
        val res = Resource.create(
            data = ByteArray(128) { 7 },
            link = link,
            advertise = false,
            autoCompress = false,
            failedCallback = { failed = true },
        )
        assertNotNull(res.callbacks.failed, "create() must install the failed callback")

        // Drive cancel() to confirm the installed callback actually fires and
        // propagates (the bug was that it fired on null and was swallowed).
        res.cancel()
        assertTrue(failed, "the failed callback installed via create() must fire on cancel")
    }

    @Test
    @DisplayName("a failure from the live watchdog fires the callback passed to create()")
    fun liveWatchdogFailureFiresCreateFailedCallback() {
        val link = freshOutLink("issue65cb2", "liverace")
        routeLinkToIface(link)
        primeWorkingLink(link)
        val fired = java.util.concurrent.CountDownLatch(1)

        // advertise=true so create() actually spawns the watchdog (the real
        // race window). The failedCallback is installed on callbacks.failed
        // BEFORE that happens.
        Resource.create(
            data = ByteArray(256) { 3 },
            link = link,
            advertise = true,
            autoCompress = false,
            failedCallback = { fired.countDown() },
        )

        // Force the running watchdog's ADVERTISED timeout to fire immediately
        // (lastActivity in the past) with no retries left (cancel branch, not
        // re-send), mirroring "link broken at create time". Pre-fix, the app
        // could not hand the callback into create(), so by the time the
        // watchdog fired, callbacks.failed was still null -> the failure was a
        // no-op and this latch never counted down (red). Post-fix the callback
        // is already installed, so the watchdog's cancel invokes it.
        val outgoingField = Link::class.java.getDeclaredField("outgoingResources")
        outgoingField.isAccessible = true
        val outgoing = @Suppress("UNCHECKED_CAST") outgoingField.get(link) as MutableList<Resource>
        assertEquals(1, outgoing.size, "create(advertise=true) must register the outgoing resource")
        val res = outgoing.first()
        set(res, Resource::class.java, "lastActivity", 0L)
        set(res, Resource::class.java, "retriesLeft", 0)

        assertTrue(fired.await(5, java.util.concurrent.TimeUnit.SECONDS),
            "the live watchdog's failure must invoke the failedCallback passed to create()")
    }

    // ===== Bug 1: sender watchdog recovery actions =====

    @Test
    @DisplayName("sender in ADVERTISED with a timeout re-sends the advertisement")
    fun senderAdvertisedTimeoutResendsAdvertisement() {
        val link = freshOutLink("issue65adv", "resend")
        primeWorkingLink(link) // 16-byte linkId before path routing
        routeLinkToIface(link)
        val res = senderResource(link)

        // Prime the ADVERTISED state: status set, advSent/lastActivity in the
        // past (so the timeout check fires), and the ADVERTISED retry budget
        // primed the way python __advertise_job does (max_adv_retries).
        set(res, Resource::class.java, "status", ResourceConstants.ADVERTISED)
        set(res, Resource::class.java, "advSent", 0L)
        set(res, Resource::class.java, "lastActivity", 0L)
        set(res, Resource::class.java, "retriesLeft", ResourceConstants.MAX_ADV_RETRIES)

        val before = countFrames(iface, PacketContext.RESOURCE_ADV.value)
        res.watchdogTickForTest()

        // The recovery action (python Resource.py:584-585) re-sends the
        // advertisement: a fresh RESOURCE_ADV frame appears at the interface,
        // and the transfer is NOT failed.
        assertEquals(before + 1, countFrames(iface, PacketContext.RESOURCE_ADV.value),
            "ADVERTISED timeout must re-send the advertisement")
        assertEquals(ResourceConstants.ADVERTISED, res.status, "a re-send must not fail the transfer")
    }

    @Test
    @DisplayName("sender in ADVERTISED with retries exhausted cancels (no re-send)")
    fun senderAdvertisedRetriesExhaustedCancels() {
        val link = freshOutLink("issue65adv2", "exhaust")
        primeWorkingLink(link) // 16-byte linkId before path routing
        routeLinkToIface(link)
        val res = senderResource(link)

        set(res, Resource::class.java, "status", ResourceConstants.ADVERTISED)
        set(res, Resource::class.java, "advSent", 0L)
        set(res, Resource::class.java, "lastActivity", 0L)
        set(res, Resource::class.java, "retriesLeft", 0) // no retries left

        val before = countFrames(iface, PacketContext.RESOURCE_ADV.value)
        res.watchdogTickForTest()

        // python Resource.py:576-578: no retries left -> cancel, no re-send.
        assertEquals(before, countFrames(iface, PacketContext.RESOURCE_ADV.value),
            "no retries left must NOT re-send the advertisement")
        assertEquals(ResourceConstants.FAILED, res.status, "ADVERTISED timeout with no retries must cancel")
    }

    @Test
    @DisplayName("sender in AWAITING_PROOF with a timeout queries the proof from the cache")
    fun senderAwaitingProofTimeoutQueriesCache() {
        val link = freshOutLink("issue65prf", "proof")
        primeWorkingLink(link) // 16-byte linkId before path routing
        routeLinkToIface(link)
        val res = senderResource(link)

        set(res, Resource::class.java, "status", ResourceConstants.AWAITING_PROOF)
        set(res, Resource::class.java, "lastPartSent", 0L)
        set(res, Resource::class.java, "lastActivity", 0L)
        set(res, Resource::class.java, "retriesLeft", ResourceConstants.MAX_RETRIES)

        val before = res.proofCacheQueriesForTest()
        res.watchdogTickForTest()

        // The recovery action (python Resource.py:651-657) queries the proof
        // from the network cache via Transport.cache_request.
        assertEquals(before + 1, res.proofCacheQueriesForTest(),
            "AWAITING_PROOF timeout must query the proof from the network cache")
        assertEquals(ResourceConstants.AWAITING_PROOF, res.status, "a proof-cache query must not fail the transfer")
    }
}
