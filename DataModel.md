LinkBench Data and Queries
==========================
Facebook has various types of object nodes, such as users, pages etc and various
types of associations between those objects. The collection of objects and
associations can be viewed as a social graph.

A goal of Facebook's database infrastructure is to store this graph in
a way to achieve good performance and high efficiency.

LinkBench is an attempt to simulate the workload of Facebook's social
graph database. This is done in two ways:

* Using database schema and operations that are similar to Facebook
    schema and operations.
* Generating data and queries which are broadly similar to the
    real production data and applications.

This document describes the *node* and *link* concepts
and outlines the concrete data representation and
various SQL statements used for operations on links.

Node
----
A node represents and object with arbitrary associated data.

    class Node {
      public long id;       // Unique identifier for node
      public int type;      // Type of node
      public long version;  // Version, incremented each change
      public int time;      // Last modification time
      public byte data[];   // Arbitrary payload data
    }

Link
----
We use the term <i>link</i> to denote an association of some type between two
objects.  The term id1 is used to denote source object of an association, and
the term id2 is used to denote destination object of an association.

Here is a simple conceptual representation of a link object coming from an
application.

The key members of a link are type, id1, id2, time (while this could be the
time of link creation, it could also be any other attribute on which we might
want to sort all id2s assocated with an id1 through this type), version,
visibility (could be VISIBILITY_DEFAULT ie visible, or VISIBILITY_HIDDEN i.e
hidden) and some link data (uninterpreted stream of bytes).

    class Link {
      public long id1;        // id of source node
      public long link_type;  // type of link
      public long id2;        // id of destination node
      public byte visibility; // is link visible?
      public byte[] data;     // arbitrary data (must be short)
      public int version;     // version of link
      public long time;       // client-defined sort key (often timestamp)
    }

SQL schema
----------
Nodes are stored in a straightforward way.  In our production databases
different types can be stored in different tables, but for LinkBench
we use one big table:

    CREATE TABLE `nodetable` (
      `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
      `type` int(10) unsigned NOT NULL,
      `version` bigint(20) unsigned NOT NULL,
      `time` int(10) unsigned NOT NULL,
      `data` mediumtext NOT NULL,
      PRIMARY KEY(`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;

Links are stored as adjacency lists.  Similarly to nodes, we use
one big table.

    CREATE TABLE `linktable` (
      `id1` bigint(20) unsigned NOT NULL DEFAULT '0',
      `id2` bigint(20) unsigned NOT NULL DEFAULT '0',
      `link_type` bigint(20) unsigned NOT NULL DEFAULT '0',
      `visibility` tinyint(3) NOT NULL DEFAULT '0',
      `data` varchar(255) NOT NULL DEFAULT '',
      `time` bigint(20) unsigned NOT NULL DEFAULT '0',
      `version` int(11) unsigned NOT NULL DEFAULT '0',
      PRIMARY KEY (`id1`,`id2`,`link_type`),
      KEY `id1_type` (`id1`,`link_type`,`visibility`,`time`,`version`,`data`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;

We also have a separate table to track link counts, in order
to allow efficient querying of link counts for long adjacency
lists.

    CREATE TABLE `counttable` (
      `id` bigint(20) unsigned NOT NULL DEFAULT '0',
      `link_type` bigint(20) unsigned NOT NULL DEFAULT '0',
      `count` int(10) unsigned NOT NULL DEFAULT '0',
      `time` bigint(20) unsigned NOT NULL DEFAULT '0',
      `version` bigint(20) unsigned NOT NULL DEFAULT '0',
      PRIMARY KEY (`id`,`link_type`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1

SQL for ADD_LINK(Link l) operation
----------------------------------

This takes a link object represented by l as argument.
Here we step through the multiple stages of the add link
transaction:

    START TRANSACTION

First we try to insert into link table.

    INSERT INTO linktable (
      id1, id2, link_type,
      visibility, data, time, version
    ) VALUES (
      l.id1, l.id2, l.link_type,
      VISIBILITY_DEFAULT, l.data, l.time. l.version
    ) ON DUPLICATE KEY UPDATE visibility = VISIBILITY_DEFAULT;

Depending upon the number of affect rows reported by MySQL
we decide whether to update count and/or data. Here is pseudocode for that:

    if (affectedrows == 0) {
      // nothing changed. A row is found but was already visible
      update remaining link fields
    } else if (affectedrows == 1) {
      // a new row was inserted
      update count table
    } else {
      // affectedrows is 2
      // link switch from hidden to visible
      update remaining link fields
      update count table
    }

Here is statement used for updating data (the first query only updates
visibility):

    UPDATE linktable SET
      visibility = VISIBILITY_DEFAULT,
      data = l.data,
      time = l.time,
      version = l.version
    WHERE id1 = l.id1 AND
      id2 = l.id2 AND
      link_type = l.link_type;

Updating counttable:

    INSERT INTO counttable(
      id, link_type, count, time, version
    ) VALUES (
      l.id1, l.link_type, 1, l.time, l.version
    ) ON DUPLICATE KEY UPDATE count = count + 1;

And finally we commit:

    COMMIT //commit transaction

SQL for UPDATE_LINK(Link l) operation
-----------------------------------

This also takes a link object as argument and we do the same thing as
ADD_LINK(l).

SQL for DELETE_LINK (id1, id2, link_type)
-----------------------------------------
We only require l.id1, l.id2 and l.link_type to delete a link.
We have the option of expunging (actually deleting), or just
hiding the lnk.

    START TRANSACTION

Updating linktable.
First do a select to check if the link is not there, is there and hidden,
or is there and visible. In case of a visible link, later we need to
mark the link as hidden, and update counttable.

    SELECT visibility FROM linktable
    WHERE id1 = <id1> AND
      id2 = <id2> AND
      link_type = <link_type>;

    if (row does not exist || link is hidden)
      // do nothing
    } else if (link is visible) {
      // either delete or mark the link as hidden
      if (expunge) {
        DELETE FROM linktable
        WHERE id1 = <id1> AND
          id2 = <id2> AND
          link_type = <link_type>;
      } else {
        UPDATE linktable SET
          visibility = VISIBILITY_HIDDEN
        WHERE id1 = <id1> AND
          id2 = <id2> AND
          link_type = <link_type>;
      }
    }

Then, if needed, we update the count table:

    if (update_count_needed) {

      INSERT INTO counttable (
        id, assoc_type, count, time, version
      ) VALUES (
        <id1>, <link_type>, 0, getSystemTime(), 0
      ) ON DUPLICATE KEY UPDATE
        count = IF (count = 0, 0, count - 1),
        time = getSystemTime(),
        version = version + 1;

    }

That finishes the transaction:

    COMMIT

SQL for COUNT_LINKS(id1, link_type) operation
---------------------------------------------

    SELECT COUNT FROM counttable
    WHERE id = <id1> AND
      link_type = <link_type>;

SQL for MULTIGET_LINK(id1, link_type, id2s) operation
-----------------------------------------------

    SELECT id1, id2, link_type, visibility, data, time, version
    FROM linktable
    WHERE id1 = <id1> AND
      id2 in (<id2s>) AND
      link_type = <link_type>;

SQL for GET_LINK_RANGE(id1, link_type, minTime, maxTime, offset, limit) operation
----------------------------------------------------------------------

    SELECT id1, id2, link_type, visibility, data, time, version FROM linktable
    WHERE id1 = <id1> AND
      link_type = <link_type> AND
      time >= <minTime> AND
      time <= <maxTimestamp> AND
      visibility = VISIBILITY_DEFAULT
    ORDER BY time DESC LIMIT <offset>, <limit>;

