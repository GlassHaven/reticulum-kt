package network.reticulum.resource

import network.reticulum.channel.StreamDataMessage
import network.reticulum.common.DestinationDirection
import network.reticulum.common.DestinationType
import network.reticulum.destination.Destination
import network.reticulum.identity.Identity
import network.reticulum.link.Link
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.io.File
import java.io.RandomAccessFile

/**
 * Unit tests for the receiver-side integrity-failure branches in
 * Resource.assemble() and the split temp-file cleanup in closeInputFile().
 *
 * These are the "corrupted-in-flight transfer must become CORRUPT, not be
 * silently accepted" branches. Each marks the transfer CORRUPT (or cancels it
 * for the decompression bomb) and fires the failed callback instead of
 * delivering bad data to the application. They are exercised directly by
 * constructing a receiver Resource and driving assembleForTest(); with
 * encrypted = false the assembly path runs without a real link handshake, and
 * every branch below returns via markCorrupt() BEFORE prove(), so a fresh
 * (never-ACTIVE) Link is sufficient. The private fields are set via reflection
 * (the same seam LinkResourceDedupTest uses for the private constructor).
 */
@DisplayName("Resource assembly integrity failures")
class ResourceAssemblyIntegrityFailureTest {

    private fun freshSingleLink(): Link {
        val dest = Destination.create(
            identity = Identity.create(),
            direction = DestinationDirection.OUT,
            type = DestinationType.SINGLE,
            appName = "assembleint",
            aspects = arrayOf("failure")
        )
        return Link.create(dest)
    }

    /** Build a receiver Resource via the private constructor (no live link). */
    private fun receiverResource(link: Link): Resource {
        val ctor = Resource::class.java.getDeclaredConstructor(Link::class.java, Boolean::class.javaPrimitiveType)
        ctor.isAccessible = true
        return ctor.newInstance(link, false) as Resource
    }

    private fun <T> setField(target: Any, name: String, value: T) {
        val f = Resource::class.java.getDeclaredField(name)
        f.isAccessible = true
        f.set(target, value)
    }

    private fun statusOf(target: Any): Int {
        val f = Resource::class.java.getDeclaredField("status")
        f.isAccessible = true
        return f.getInt(target)
    }

    @Test
    @DisplayName("a missing part makes the assembled resource CORRUPT, not delivered")
    fun `missing part marks corrupt`() {
        val res = receiverResource(freshSingleLink())
        setField(res, "status", ResourceConstants.TRANSFERRING)
        setField(res, "encrypted", false)
        setField(res, "compressed", false)
        setField(res, "randomHash", ByteArray(4) { 7 })
        // A null part is a hole in the stream: python's b"".join raises,
        // landing in the CORRUPT branch (Resource.py:676/721).
        setField(res, "parts", arrayOf(ByteArray(8) { 1 }, null))
        val failed = java.util.concurrent.atomic.AtomicBoolean(false)
        res.callbacks.failed = { failed.set(true) }

        res.assembleForTest()

        assertEquals(ResourceConstants.CORRUPT, statusOf(res))
        assertTrue(failed.get(), "the failed callback must fire on a corrupt assembly")
    }

    @Test
    @DisplayName("a stream too short to carry the random prefix is CORRUPT")
    fun `data too short marks corrupt`() {
        val res = receiverResource(freshSingleLink())
        setField(res, "status", ResourceConstants.TRANSFERRING)
        setField(res, "encrypted", false)
        setField(res, "compressed", false)
        setField(res, "randomHash", ByteArray(4) { 7 })
        // Fewer than RANDOM_HASH_SIZE (4) bytes after decryption.
        setField(res, "parts", arrayOf(ByteArray(3)))

        res.assembleForTest()

        assertEquals(ResourceConstants.CORRUPT, statusOf(res))
    }

    @Test
    @DisplayName("a hash mismatch (tampered content) makes the resource CORRUPT")
    fun `hash mismatch marks corrupt`() {
        val res = receiverResource(freshSingleLink())
        setField(res, "status", ResourceConstants.TRANSFERRING)
        setField(res, "encrypted", false)
        setField(res, "compressed", false)
        setField(res, "randomHash", ByteArray(4) { 7 })
        // 4-byte prefix + 8 content bytes, but the hash is WRONG.
        setField(res, "parts", arrayOf(ByteArray(4) { 0 } + ByteArray(8) { 1 }))
        setField(res, "hash", ByteArray(32) { 0xAA.toByte() }) // wrong on purpose

        res.assembleForTest()

        assertEquals(ResourceConstants.CORRUPT, statusOf(res))
    }

    @Test
    @DisplayName("an authenticated-encryption failure (decrypt returns null) is CORRUPT")
    fun `decryption failure marks corrupt`() {
        // With encrypted = true, assemble() calls link.decrypt(). A fresh
        // (never-ACTIVE) link has no derived key, so Token(derivedKey!!) throws
        // and decrypt() returns null (its catch path) - the exact shape of an
        // HMAC/auth failure on a tampered-in-flight part. That must be CORRUPT,
        // not a crash and not a silent accept.
        val res = receiverResource(freshSingleLink())
        setField(res, "status", ResourceConstants.TRANSFERRING)
        setField(res, "encrypted", true)
        setField(res, "compressed", false)
        setField(res, "randomHash", ByteArray(4) { 7 })
        setField(res, "parts", arrayOf(ByteArray(16) { 1 }))

        res.assembleForTest()

        assertEquals(ResourceConstants.CORRUPT, statusOf(res))
    }

    @Test
    @DisplayName("a bz2 stream that inflates past the bound is a decompression bomb: CORRUPT")
    fun `decompression bomb marks corrupt`() {
        val res = receiverResource(freshSingleLink())
        setField(res, "status", ResourceConstants.TRANSFERRING)
        setField(res, "encrypted", false)
        setField(res, "compressed", true)
        setField(res, "randomHash", ByteArray(4) { 7 })
        // A genuine bz2 stream that inflates well past the bound.
        val inflated = ByteArray(100_000) { (it % 256).toByte() }
        val compressedStream = StreamDataMessage.compressForTest(inflated)
        res.setMaxDecompressedSizeForTest(1024) // far below the 100 KB inflation
        // 4-byte random prefix + the compressed stream.
        setField(res, "parts", arrayOf(ByteArray(4) { 0 } + compressedStream))

        res.assembleForTest()

        // The bomb guard marks the transfer CORRUPT. (It also calls cancel(), but
        // markCorrupt runs first and sets status to CORRUPT, which is already a
        // terminal state (>= COMPLETE), so cancel() is a no-op here and is not
        // observable; only the CORRUPT transition is asserted.)
        assertEquals(ResourceConstants.CORRUPT, statusOf(res))
    }

    @Test
    @DisplayName("cancel() releases the split temp file and input file (closeInputFile)")
    fun `cancel closes and deletes the split temp file`() {
        val res = receiverResource(freshSingleLink())
        setField(res, "status", ResourceConstants.TRANSFERRING)
        // Back a split transfer's input file / temp file via reflection (both
        // are private), then cancel and confirm closeInputFile released them.
        val temp = File.createTempFile("rns-res-test", ".tmp")
        temp.writeBytes(ByteArray(16) { 3 })
        val input = RandomAccessFile(temp, "rw")
        setField(res, "tempFile", temp)
        setField(res, "inputFile", input)

        val failed = java.util.concurrent.atomic.AtomicBoolean(false)
        res.callbacks.failed = { failed.set(true) }

        res.cancel()

        assertEquals(ResourceConstants.FAILED, statusOf(res))
        assertTrue(failed.get(), "the failed callback must fire on cancel")
        assertTrue(!temp.exists(), "closeInputFile should delete the split temp file")
    }
}
