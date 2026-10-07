package network.reticulum.interop.resource

import network.reticulum.interop.RnsLiveTestBase
import network.reticulum.interop.getBoolean
import network.reticulum.link.Link
import network.reticulum.link.LinkConstants
import network.reticulum.resource.Resource
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * E2E tests for Resource transfer between Kotlin and Python over a Link.
 *
 * Resources handle automatic chunking, compression, and retransmission
 * for reliable transfer of arbitrary-sized data over an encrypted link.
 */
@DisplayName("Resource Transfer E2E Tests")
class ResourceTransferE2ETest : RnsLiveTestBase() {

    /**
     * Establish a link and wait for both sides to be ready.
     * Returns the active link.
     */
    private fun establishLink(): Link {
        val destination = createPythonOutDestination()

        val establishedLatch = CountDownLatch(1)
        var link: Link? = null

        Link.create(
            destination,
            establishedCallback = { l ->
                link = l
                establishedLatch.countDown()
            }
        )

        assertTrue(establishedLatch.await(15, TimeUnit.SECONDS), "Link should establish")
        waitForPythonLink(5000)
        assertNotNull(link)
        assertEquals(LinkConstants.ACTIVE, link!!.status)

        return link!!
    }

    @Test
    @DisplayName("Kotlin can send resource to Python over link")
    @Timeout(60)
    fun `kotlin can send resource to python over link`() {
        val link = establishLink()

        // Set resource strategy to accept all on Kotlin side too
        link.setResourceStrategy(Link.ACCEPT_ALL)

        val testData = "Hello Resource from Kotlin! This is a test payload.".toByteArray()
        val completedLatch = CountDownLatch(1)
        val completedResource = AtomicReference<Resource>()

        println("  [Test] Sending ${testData.size} byte resource from Kotlin to Python...")

        val resource = Resource.create(
            data = testData,
            link = link,
            callback = { r ->
                completedResource.set(r)
                completedLatch.countDown()
            }
        )

        assertNotNull(resource)

        // Wait for transfer to complete on Kotlin side
        val completed = completedLatch.await(30, TimeUnit.SECONDS)
        assertTrue(completed, "Resource transfer should complete within 30 seconds")

        // Poll Python for received resource
        val deadline = System.currentTimeMillis() + 15_000
        var pythonResources: List<ReceivedResource> = emptyList()
        while (System.currentTimeMillis() < deadline) {
            pythonResources = getPythonResources()
            if (pythonResources.isNotEmpty()) break
            Thread.sleep(500)
        }

        assertTrue(pythonResources.isNotEmpty(), "Python should receive the resource")
        assertTrue(
            testData.contentEquals(pythonResources[0].data),
            "Received resource data should match sent data"
        )
        println("  [Test] Kotlin → Python resource transfer verified!")

        link.teardown()
    }

    @Test
    @DisplayName("Python can send resource to Kotlin over link")
    @Timeout(60)
    fun `python can send resource to kotlin over link`() {
        val link = establishLink()

        // Configure Kotlin side to accept resources
        link.setResourceStrategy(Link.ACCEPT_ALL)

        val receivedResources = CopyOnWriteArrayList<ByteArray>()
        val receivedLatch = CountDownLatch(1)

        link.callbacks.resourceConcluded = { resourceObj ->
            val res = resourceObj as? Resource
            if (res != null) {
                val data = res.data
                if (data != null) {
                    receivedResources.add(data)
                    receivedLatch.countDown()
                }
            }
        }

        // Send resource from Python
        val testData = "Hello Resource from Python! This is a test payload.".toByteArray()
        println("  [Test] Sending ${testData.size} byte resource from Python to Kotlin...")
        val sendResult = python("rns_resource_send", "data" to testData)
        assertTrue(sendResult.getBoolean("sent"), "Python resource send should succeed")

        // Wait for Kotlin to receive
        val received = receivedLatch.await(30, TimeUnit.SECONDS)
        assertTrue(received, "Kotlin should receive the resource within 30 seconds")

        assertTrue(receivedResources.isNotEmpty(), "Kotlin should have received resource data")
        assertTrue(
            testData.contentEquals(receivedResources[0]),
            "Received resource data should match sent data"
        )
        println("  [Test] Python → Kotlin resource transfer verified!")

        link.teardown()
    }

    @Test
    @DisplayName("Large resource from Kotlin triggers multi-part transfer")
    @Timeout(90)
    fun `large resource from kotlin triggers multi-part transfer`() {
        val link = establishLink()

        link.setResourceStrategy(Link.ACCEPT_ALL)

        // Create data larger than a single SDU (~464 bytes) to force multi-part
        val testData = ByteArray(2000) { (it % 256).toByte() }
        val completedLatch = CountDownLatch(1)
        val completedResource = AtomicReference<Resource>()

        println("  [Test] Sending ${testData.size} byte resource from Kotlin (should be multi-part)...")

        val resource = Resource.create(
            data = testData,
            link = link,
            callback = { r ->
                completedResource.set(r)
                completedLatch.countDown()
            }
        )

        assertNotNull(resource)

        val completed = completedLatch.await(60, TimeUnit.SECONDS)
        assertTrue(completed, "Large resource transfer should complete within 60 seconds")

        // Poll Python for received resource
        val deadline = System.currentTimeMillis() + 15_000
        var pythonResources: List<ReceivedResource> = emptyList()
        while (System.currentTimeMillis() < deadline) {
            pythonResources = getPythonResources()
            if (pythonResources.isNotEmpty()) break
            Thread.sleep(500)
        }

        assertTrue(pythonResources.isNotEmpty(), "Python should receive large resource")
        assertTrue(
            testData.contentEquals(pythonResources[0].data),
            "Received large resource data should match (${testData.size} bytes)"
        )
        println("  [Test] Kotlin → Python large resource transfer verified! (${testData.size} bytes)")

        link.teardown()
    }

    @Test
    @DisplayName("Large resource from Python triggers multi-part transfer")
    @Timeout(90)
    fun `large resource from python triggers multi-part transfer`() {
        val link = establishLink()

        // Configure Kotlin side to accept resources
        link.setResourceStrategy(Link.ACCEPT_ALL)

        val receivedResources = CopyOnWriteArrayList<ByteArray>()
        val receivedLatch = CountDownLatch(1)

        link.callbacks.resourceConcluded = { resourceObj ->
            val res = resourceObj as? Resource
            if (res != null) {
                val data = res.data
                if (data != null) {
                    receivedResources.add(data)
                    receivedLatch.countDown()
                }
            }
        }

        // Create data larger than a single SDU to force multi-part transfer
        // Resource SDU is ~464 bytes (MTU - HEADER_MAX - IFAC_MIN), so 2000 bytes forces multi-part
        val testData = ByteArray(2000) { (it % 256).toByte() }

        println("  [Test] Sending ${testData.size} byte resource from Python (should be multi-part)...")
        val sendResult = python("rns_resource_send", "data" to testData)
        assertTrue(sendResult.getBoolean("sent"), "Python resource send should succeed")

        // Wait for Kotlin to receive
        val received = receivedLatch.await(60, TimeUnit.SECONDS)
        assertTrue(received, "Kotlin should receive large resource within 60 seconds")

        assertTrue(receivedResources.isNotEmpty(), "Kotlin should have received resource data")
        assertTrue(
            testData.contentEquals(receivedResources[0]),
            "Received large resource data should match (${testData.size} bytes)"
        )
        println("  [Test] Multi-part resource transfer verified! (${testData.size} bytes)")

        link.teardown()
    }

    @Test
    @DisplayName("In-memory resource above MAX_EFFICIENT_SIZE completes as a split transfer")
    @Timeout(180)
    fun `split in-memory resource from kotlin completes to python`() {
        val link = establishLink()

        link.setResourceStrategy(Link.ACCEPT_ALL)

        // A payload above MAX_EFFICIENT_SIZE (1 MiB - 1) is split into multiple
        // segments. The Kotlin sender must spill the in-memory payload to a
        // temporary file and prepare each continuation segment from that file as
        // the previous segment's proof arrives - the F3.1 path. Without it, the
        // sender advertised the whole payload as one transfer, prepared no next
        // segment, and the proof validator waited for a segment that never
        // existed, spinning the inbound thread under the node jobs lock (remote
        // DoS) until the transfer timed out.
        val testData = ByteArray(1024 * 1024 + 512) { (it % 251).toByte() }
        val completedLatch = CountDownLatch(1)
        val completedResource = AtomicReference<Resource>()

        println("  [Test] Sending ${testData.size} byte resource from Kotlin (should split into 2 segments)...")

        val resource = Resource.create(
            data = testData,
            link = link,
            callback = { r ->
                completedResource.set(r)
                completedLatch.countDown()
            }
        )

        assertNotNull(resource)
        assertTrue(resource.split, "payload above MAX_EFFICIENT_SIZE must be a split transfer")
        assertEquals(2, resource.totalSegments, "1 MiB + 512 bytes splits into 2 segments")

        val completed = completedLatch.await(120, TimeUnit.SECONDS)
        assertTrue(completed, "Split resource transfer should complete within 120 seconds")

        // Poll Python for the assembled resource.
        val deadline = System.currentTimeMillis() + 30_000
        var pythonResources: List<ReceivedResource> = emptyList()
        while (System.currentTimeMillis() < deadline) {
            pythonResources = getPythonResources()
            if (pythonResources.isNotEmpty()) break
            Thread.sleep(500)
        }

        assertTrue(pythonResources.isNotEmpty(), "Python should receive the split resource")
        assertTrue(
            testData.contentEquals(pythonResources[0].data),
            "Assembled split resource data should match (${testData.size} bytes)"
        )
        println("  [Test] Kotlin -> Python split resource transfer verified! (${testData.size} bytes, ${resource.totalSegments} segments)")

        link.teardown()
    }

    @Test
    @DisplayName("Python sends a split resource to Kotlin over link")
    @Timeout(180)
    fun `split resource from python completes on kotlin`() {
        val link = establishLink()

        link.setResourceStrategy(Link.ACCEPT_ALL)

        // The receiver's split path: each segment arrives as its own incoming
        // Resource (initializeFromAdvertisement builds it from the advertisement),
        // and assemble() reassembles that segment's parts. The Kotlin->Python
        // test covers the SENDER split path (temp-file spill + per-segment
        // preparation); this covers the RECEIVER side (per-segment initialize +
        // assemble).
        //
        // Note: the Kotlin receiver delivers each segment to the application as a
        // SEPARATE Resource - it does not stitch segments into one payload
        // (the pre-existing, separately-xfail'd "no segment-append" gap). We
        // therefore collect every segment in order and concatenate here, which
        // still exercises the per-segment receiver path and proves each segment's
        // content is correct.
        val segments = HashMap<Int, ByteArray>()
        var totalSegmentsSeen = 0
        var maxSegmentIndex = 0
        val allArrived = CountDownLatch(1)

        link.callbacks.resourceConcluded = { resourceObj ->
            val res = resourceObj as? Resource
            if (res != null) {
                val data = res.data
                if (data != null) {
                    segments[res.segmentIndex] = data
                    totalSegmentsSeen = res.totalSegments
                    maxSegmentIndex = maxOf(maxSegmentIndex, res.segmentIndex)
                    if (maxSegmentIndex == totalSegmentsSeen) {
                        allArrived.countDown()
                    }
                }
            }
        }

        // A payload above MAX_EFFICIENT_SIZE (1 MiB - 1) so Python splits it
        // into 2 segments, which Kotlin must receive and assemble per-segment.
        val testData = ByteArray(1024 * 1024 + 512) { (it % 251).toByte() }

        println("  [Test] Sending ${testData.size} byte resource from Python (should split into 2 segments)...")
        val sendResult = python("rns_resource_send", "data" to testData)
        assertTrue(sendResult.getBoolean("sent"), "Python resource send should succeed")

        val received = allArrived.await(120, TimeUnit.SECONDS)
        assertTrue(received, "Kotlin should receive all split segments within 120 seconds")

        assertEquals(2, totalSegmentsSeen, "payload above MAX_EFFICIENT_SIZE must be a 2-segment split")
        assertEquals(2, segments.size, "both segments should have been received and assembled")

        // Reassemble by segment order and verify byte-exactness.
        val reassembled = segments.entries.sortedBy { it.key }.flatMap { it.value.toList() }.toByteArray()
        assertEquals(testData.size, reassembled.size, "reassembled size should match the payload")
        assertTrue(
            reassembled.contentEquals(testData),
            "concatenated split segments should match the payload (${testData.size} bytes)"
        )
        println("  [Test] Python -> Kotlin split resource transfer verified! (${testData.size} bytes, ${segments.size} segments)")

        link.teardown()
    }
}
