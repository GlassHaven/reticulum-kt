package network.reticulum.resource

import network.reticulum.common.DestinationDirection
import network.reticulum.common.DestinationType
import network.reticulum.destination.Destination
import network.reticulum.identity.Identity
import network.reticulum.link.Link
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

/**
 * Unit tests for the F3.1 split first-segment metadata-budget guard.
 *
 * The first segment's RAW read is MAX_EFFICIENT_SIZE - metadataBlockSize
 * (python first_read_size, Resource.py:303). A metadata block that fills or
 * exceeds the whole segment budget makes that read size <= 0: the reference
 * then reads a negative size and CPython raises ValueError, so the reference
 * does not complete this degenerate transfer either. The Kotlin guard fails
 * fast with a clear error BEFORE touching the temporary file, instead of a
 * mid-init copyOfRange exception after the file had already been opened.
 *
 * The guard throws before link.encrypt, so a fresh (never-ACTIVE) Link is
 * sufficient - no live Transport or handshake is needed.
 */
@DisplayName("Resource split metadata budget guard")
class ResourceSplitMetadataBudgetGuardTest {

    private fun freshSingleLink(): Link {
        val dest = Destination.create(
            identity = Identity.create(),
            direction = DestinationDirection.OUT,
            type = DestinationType.SINGLE,
            appName = "splitmeta",
            aspects = arrayOf("guard")
        )
        return Link.create(dest)
    }

    @Test
    @DisplayName("metadata block filling the first-segment budget fails fast before the temp file")
    fun `oversized metadata for a split resource throws IllegalArgumentException`() {
        // A payload above MAX_EFFICIENT_SIZE (1 MiB - 1) so the resource is a
        // split transfer (totalSize > MAX_EFFICIENT_SIZE). The metadata must be
        // accepted (<= METADATA_MAX_SIZE = 1048575) yet pack to a block
        // >= MAX_EFFICIENT_SIZE: for a >65535-byte value msgpack uses a bin32
        // header, so metadataBlockSize = 3 (length prefix) + 5 (bin32 header)
        // + rawSize = 8 + rawSize. rawSize = 1048570 gives 1048578 >= 1048575,
        // making firstChunk (MAX_EFFICIENT_SIZE - metadataBlockSize) negative.
        val data = ByteArray(1024 * 1024 + 512)
        val metadata = ByteArray(1048570)
        // Sanity: this is within the accepted range and over the segment budget.
        assertTrue(metadata.size <= ResourceConstants.METADATA_MAX_SIZE)
        assertTrue(8 + metadata.size >= ResourceConstants.MAX_EFFICIENT_SIZE)

        val ex = assertThrows(IllegalArgumentException::class.java) {
            Resource.create(
                data = data,
                link = freshSingleLink(),
                metadata = metadata,
                advertise = false,
                autoCompress = false
            )
        }
        assertTrue(
            ex.message!!.contains("leaves no room for the first segment payload"),
            "expected the metadata-budget guard message, got: ${ex.message}"
        )
    }

    @Test
    @DisplayName("a metadata block below the budget does not trip the guard")
    fun `normal metadata for a split resource does not fire the guard`() {
        // Same split payload, but a small metadata block: metadataBlockSize is
        // far below MAX_EFFICIENT_SIZE, so firstChunk stays positive and the
        // guard must not fire. A fresh (never-ACTIVE) link's encrypt would
        // throw (no derivedKey) only AFTER the guard, so if any failure occurs
        // it must not be the guard's message.
        val data = ByteArray(1024 * 1024 + 512)
        val metadata = ByteArray(64)

        val result = runCatching {
            Resource.create(
                data = data,
                link = freshSingleLink(),
                metadata = metadata,
                advertise = false,
                autoCompress = false
            )
        }
        val msg = result.exceptionOrNull()?.message
        if (msg != null) {
            assertTrue(
                !msg.contains("leaves no room for the first segment payload"),
                "guard fired incorrectly for a below-budget metadata block: $msg"
            )
        }
    }
}
