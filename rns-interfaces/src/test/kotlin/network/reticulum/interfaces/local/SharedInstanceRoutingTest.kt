package network.reticulum.interfaces.local

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Tests for shared instance routing behavior.
 *
 * Verifies critical fix for Kotlin-Python interoperability where
 * Columba (Python RNS client) connected to Kotlin shared instance
 * received zero announces.
 *
 * Root cause: Python immediately retransmits announces to local
 * client interfaces (Transport.py:1697-1742), bypassing announce
 * queue and rate limiting. Kotlin was queuing announces, causing
 * them to get stuck in rate-limited queues.
 *
 * Ports: every server is bound to an ephemeral port (tcpPort = 0) and the
 * clients connect to server!!.boundPort. Fixed literals (the old 37434-37441)
 * fall inside the OS ephemeral range, so a bind could collide with an
 * ephemeral allocation or a sibling test's socket and fail with
 * BindException in CI (same flake class #96 eliminated in LocalInterfaceTest).
 */
class SharedInstanceRoutingTest {

    private var server: LocalServerInterface? = null
    private val clients = mutableListOf<LocalClientInterface>()

    @AfterEach
    fun tearDown() {
        clients.forEach { it.detach() }
        clients.clear()
        server?.detach()
        server = null
        Thread.sleep(100)
    }

    @Test
    fun `server has isLocalSharedInstance set to true`() {
        server = LocalServerInterface(name = "TestServer", tcpPort = 0)

        // LocalServerInterface should identify as a shared instance server
        assertTrue(server!!.isLocalSharedInstance)
    }

    @Test
    fun `client interfaces spawned by server are tracked`() {
        server = LocalServerInterface(name = "TestServer", tcpPort = 0)
        server!!.start()
        val tcpPort = server!!.boundPort

        // Connect clients
        val client1 = LocalClientInterface(name = "Client1", tcpPort = tcpPort)
        val client2 = LocalClientInterface(name = "Client2", tcpPort = tcpPort)
        clients.add(client1)
        clients.add(client2)
        client1.start()
        client2.start()

        Thread.sleep(200)

        // Server should track spawned client interfaces
        val spawned = server!!.spawnedInterfaces
        assertNotNull(spawned)
        assertEquals(2, spawned?.size)
        assertEquals(2, server!!.clientCount())
    }

    @Test
    fun `spawned client interfaces have parent reference`() {
        server = LocalServerInterface(name = "TestServer", tcpPort = 0)
        server!!.start()
        val tcpPort = server!!.boundPort

        val client = LocalClientInterface(name = "Client", tcpPort = tcpPort)
        clients.add(client)
        client.start()

        Thread.sleep(200)

        // Get spawned interface from server
        val spawned = server!!.spawnedInterfaces?.firstOrNull()
        assertNotNull(spawned)

        // Spawned interface should have parent reference to server
        assertEquals(server, spawned?.parentInterface)
    }

    @Test
    fun `server processOutgoing is no-op to prevent double-send`() {
        // This test verifies that LocalServerInterface.processOutgoing()
        // is a no-op because Transport calls each spawned client's
        // processOutgoing() directly.
        //
        // If server also broadcasts, clients receive packets TWICE,
        // causing data corruption.
        //
        // Python reference: LocalInterface.py:454-455

        val testData = "Test data".toByteArray()
        val receivedLatch = CountDownLatch(1)
        var receivedCount = 0

        server = LocalServerInterface(name = "TestServer", tcpPort = 0)
        server!!.start()
        val tcpPort = server!!.boundPort

        val client = LocalClientInterface(name = "Client", tcpPort = tcpPort)
        client.onPacketReceived = { _, _ ->
            receivedCount++
            receivedLatch.countDown()
        }
        clients.add(client)
        client.start()

        Thread.sleep(200)

        // Call server.processOutgoing() directly (simulating Transport call)
        server!!.processOutgoing(testData)

        // Client should NOT receive anything (server processOutgoing is no-op)
        val received = receivedLatch.await(500, TimeUnit.MILLISECONDS)

        // No data should have been received
        assertFalse(received, "Client should not receive data when server.processOutgoing() is called")
        assertEquals(0, receivedCount)
    }

    @Test
    fun `client processOutgoing sends data correctly`() {
        // Verify that spawned client interfaces can send data to external clients.
        // In production, Transport calls each spawned client's processOutgoing() directly.

        // Data must be > HEADER_MIN_SIZE (19) bytes to pass HDLC deframer validation
        val testData = "Test data from server!".toByteArray()
        val receivedLatch = CountDownLatch(1)
        var receivedData: ByteArray? = null

        server = LocalServerInterface(name = "TestServer", tcpPort = 0)
        server!!.start()
        val tcpPort = server!!.boundPort

        val client = LocalClientInterface(name = "Client", tcpPort = tcpPort)
        client.onPacketReceived = { data, _ ->
            receivedData = data
            receivedLatch.countDown()
        }
        clients.add(client)
        client.start()

        Thread.sleep(200)

        // Get spawned client interface from server
        val spawnedClient = server!!.spawnedInterfaces?.firstOrNull()
        assertNotNull(spawnedClient)

        // Transport would call spawned client's processOutgoing() directly
        spawnedClient!!.processOutgoing(testData)

        // External client should receive the data
        assertTrue(receivedLatch.await(1, TimeUnit.SECONDS))
        assertArrayEquals(testData, receivedData)
    }

    @Test
    fun `spawned clients tracked in spawnedInterfaces list`() {
        server = LocalServerInterface(name = "TestServer", tcpPort = 0)
        server!!.start()
        val tcpPort = server!!.boundPort

        // Initially no spawned interfaces
        assertTrue(server!!.spawnedInterfaces?.isEmpty() ?: true)

        // Connect first client
        val client1 = LocalClientInterface(name = "Client1", tcpPort = tcpPort)
        clients.add(client1)
        client1.start()
        Thread.sleep(200)

        assertEquals(1, server!!.spawnedInterfaces?.size)

        // Connect second client
        val client2 = LocalClientInterface(name = "Client2", tcpPort = tcpPort)
        clients.add(client2)
        client2.start()
        Thread.sleep(200)

        assertEquals(2, server!!.spawnedInterfaces?.size)

        // Disconnect first client
        client1.detach()
        Thread.sleep(200)

        assertEquals(1, server!!.spawnedInterfaces?.size)
    }

    @Test
    fun `spawned client has isConnectedToSharedInstance method`() {
        server = LocalServerInterface(name = "TestServer", tcpPort = 0)
        server!!.start()
        val tcpPort = server!!.boundPort

        val client = LocalClientInterface(name = "Client", tcpPort = tcpPort)
        clients.add(client)
        client.start()

        Thread.sleep(200)

        val spawnedClient = server!!.spawnedInterfaces?.firstOrNull()
        assertNotNull(spawnedClient)

        // Spawned client is server-side, NOT a client connecting to shared instance.
        // Python: spawned.is_connected_to_shared_instance = False
        if (spawnedClient is LocalClientInterface) {
            assertFalse(spawnedClient.isConnectedToSharedInstance())
        }
    }

    @Test
    fun `multiple clients can connect simultaneously`() {
        val numClients = 5

        server = LocalServerInterface(name = "TestServer", tcpPort = 0)
        server!!.start()
        val tcpPort = server!!.boundPort

        // Connect multiple clients in parallel
        repeat(numClients) { i ->
            val client = LocalClientInterface(name = "Client$i", tcpPort = tcpPort)
            clients.add(client)
            client.start()
        }

        // Poll until all clients have been accepted and spawned (the accept
        // thread runs handleNewClient, so clientCount() lags the synchronous
        // client.start() calls under load; a fixed sleep here is a flake).
        val deadline = System.currentTimeMillis() + 5000
        while (System.currentTimeMillis() < deadline && server!!.clientCount() < numClients) {
            Thread.sleep(10)
        }

        // All clients should be connected and tracked
        assertEquals(numClients, server!!.clientCount())
        assertEquals(numClients, server!!.spawnedInterfaces?.size)

        // All spawned interfaces should have parent reference
        server!!.spawnedInterfaces?.forEach { spawned ->
            assertEquals(server, spawned.parentInterface)
        }
    }

    @Test
    fun `server disconnection cleans up all clients`() {
        server = LocalServerInterface(name = "TestServer", tcpPort = 0)
        server!!.start()
        val tcpPort = server!!.boundPort

        // Connect clients
        repeat(3) { i ->
            val client = LocalClientInterface(name = "Client$i", tcpPort = tcpPort)
            clients.add(client)
            client.start()
        }

        Thread.sleep(200)
        assertEquals(3, server!!.clientCount())

        // Disconnect server
        server!!.detach()
        Thread.sleep(200)

        // All clients should be disconnected
        clients.forEach { client ->
            assertFalse(client.online.value)
        }
    }
}
