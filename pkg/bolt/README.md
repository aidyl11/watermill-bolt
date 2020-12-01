# Watermill BOLT Pub/Sub

BOLT Pub / Sub runs queries on a DBbolt basis, using it as a messaging system.
The BOLT subscriber executes a SELECT query for short periods of time, remembering the position of the last record.

The Bolt editor simply inserts the consumed messages into the chosen table.
a file_test.db file, to view this file using the following command "boltbrowser file_test.db"
How the package works: for the moment this code saves the data sent from the publisher to the subscriber in a DB file (dbbolt database).

This package is suitable for "test_pubsub .go" watermill function tests, the tests are not 100% valid, at a certain point of execution and a logger panic !!.

We are still working on this code to fix the little bugs that remain.


