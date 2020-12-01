# Watermill BOLT Pub/Sub

BOLT Pub / Sub driver relies on a DBbolt layer, using it as a messaging system.

The BOLT driver subscriber executes a SELECT query for short periods of time, remembering the position of the last record. The package is functional : up to the moment, this code saves the data sent from the publisher to the subscriber in a DB file (dbbolt database).

The BOLT driver test simply inserts the consumed messages into the chosen table. A file_test.db file, to view this file, you can use the boltbrowser tool. (ie "boltbrowser file_test.db"). This driver package is suitable for "test_pubsub .go" watermill functional test suite. Currently, the first test (the essential one) is validated, at a certain point of execution and a logger panic because we don’t support multiple open queries for the same DB file – a main limitation of BOLT.

The main limitation of our code as using BOLT as a base layer is that it doesn’t support multiple locks on the same DB file.We are still working on this code to fix it.

