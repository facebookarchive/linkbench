Overview

Facebook has various types of objects, such as users, pages etc and various
types of associations between those objects. The collection of objects and
assocations can be viewed as an assocication graph.

The goal of Facebook's database is to store this graph in a way to achieve
good performance and high efficiency.

LinkBench is an attempt to simulate the workload of Facebook's association
graph database. This will be done in multiple steps:

(A) Using database schema and operations that are similar to Facebook
    schema and operations. However, in this stage, the distribution of
    #links, #writes and #reads is chosen artifically.
(B) Come up with a better distribution for #links, #reads and #writes,
    hopefully something that is closer to real distribution.

Currently this benchmark just does (A).

This document briefly describes <i>link</i>, a concept
to represent association, and various SQL statements used underneath for
operations on links.

I. Link

We use the term <i>link</i> to denote an association of some type between two
objects.  The term id1 is used to denote source object of an association, and
the term id2 is used to denote destination object of an association.

Here is a simple conceptual representation of a link object coming from an
application:

The key members of a link are type, id1, id2, time (while this could be the
time of link creation, it could also be any other attribute on which we might
want to sort all id2s assocated with an id1 through this type), version,
visibility (could be VISIBILITY_DEFAULT ie visible, or VISIBILITY_HIDDEN i.e
hidden) and some link data (uninterpreted stream of bytes).

class Link {
  public long id1;
  public long link_type;
  public long id2;
  public int id1_type;
  public int id2_type;
  public byte visibility;
  public byte[] data;
  public int version;
  public long time;
}

II. SQL schema and commands

1. SQL schema to create table <i>linktable</i> for storing links:

Links are stored as adjacency lists
(http://en.wikipedia.org/wiki/Adjacency_list) on our MySQL dbs. They are
stored in multiple tables, with each table storing one or more related link
types.

CREATE database LinkBench;
use LinkBench;

CREATE TABLE `linktable` (
  `id1` bigint(20) unsigned NOT NULL DEFAULT '0',
  `id1_type` int(10) unsigned NOT NULL DEFAULT '0',
  `id2` bigint(20) unsigned NOT NULL DEFAULT '0',
  `id2_type` int(10) unsigned NOT NULL DEFAULT '0',
  `link_type` bigint(20) unsigned NOT NULL DEFAULT '0',
  `visibility` tinyint(3) NOT NULL DEFAULT '0',
  `data` varchar(255) NOT NULL DEFAULT '',
  `time` bigint(20) unsigned NOT NULL DEFAULT '0',
  `version` int(11) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`id1`,`id2`,`link_type`),
  KEY `id2_vis` (`id2`,`visibility`),
  KEY `id1_type` (`id1`,`link_type`,`visibility`,`time`,`version`,`data`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


2. SQL schema to create table <i>counttable</i> for tracking link counts:

CREATE TABLE `counttable` (
  `id` bigint(20) unsigned NOT NULL DEFAULT '0',
  `id_type` int(10) unsigned NOT NULL DEFAULT '0',
  `link_type` bigint(20) unsigned NOT NULL DEFAULT '0',
  `count` int(10) unsigned NOT NULL DEFAULT '0',
  `time` bigint(20) unsigned NOT NULL DEFAULT '0',
  `version` bigint(20) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`,`link_type`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1

3. SQL for ADD_LINK(Link l) operation

This takes a link object represented by l as argument.

START TRANSACTION

Updating linktable:

INSERT INTO linktable (
  id1, id1_type, id2, id2_type, link_type,
  visibility, data, time, version
) VALUES (
  l.id1, l.id1_type, l.id2, l.id2_type, l.link_type,
  VISIBILITY_DEFAULT, l.data, l.time. l.version
) ON DUPLICATE KEY UPDATE visibility = VISIBILITY_DEFAULT;

Depending upon number of rows affected we decide whether to update count
and/or data. Here is pseudocode for that:

switch (affectedrows) {
  case 1:
    // a new row was inserted --> need to update counttable
    update_count = true; break;

  case 2:
    // nothing changed (a row is found but its visibility was already
    // VISIBILITY_DEFAULT) --> need to update other data
    update_data = true; break;

  case 3:
    // a visibility was changed from VISIBILITY_HIDDEN to DEFAULT --> need to
    // update both counttable and other data
    update_count = true; update_data = true; break;

  default: System.err.print("SQL Error"); throw new Exception("Value of
  affected-rows number is not valid" + affactedrows);
}

Here is statement used for updating data (the first query only updates
visibility):

UPDATE linktable SET
  id1_type = l.id1_type,
  id2_type = l.id2_type,
  visibility = VISIBILITY_DEFAULT,
  data = l.data,
  time = l.time,
  version = l.version
WHERE id1 = l.id1 AND
  id2 = " + l.id2 AND
  link_type = l.link_type;

Updating countable:

if (update_count_needed) {

  INSERT INTO counttable(
    id, id_type, link_type, count, time, version
  ) VALUES (
    l.id1, l.id1_type, l.link_type, 1, l.time, l.version
  ) ON DUPLICATE KEY UPDATE count = count + 1;

}

COMMIT //commit transaction


4. SQL for UPDATE_LINK(Link l) operation

This also takes a link object as argument and we do the samething as
ADD_LINK(l).

5. SQL for DELETE_LINK (id1, id2, link_type) - only require l.id1, l.id2 and l.link_type to delete a link

START TRANSACTION

Updating linktable

// First do a select to check if the link is not there, is there and hidden,
// or is there and visible; Result could be either NULL, VISIBILITY_HIDDEN
// or VISIBILITY_DEFAULT.  In case of VISIBILITY_DEFAULT, later we need to
// mark the link as hidden, and update counttable.

SELECT visibility FROM linktable
WHERE id1 = <id1> AND
  id2 = <id2> AND
  link_type = <link_type>;

if (row does not exist) OR  (visibility == VISIBILITY_HIDDEN)  {
  // do nothing
} else if (visibility == VISIBILITY_DEFAULT) {
  // either delete or mark the link as hidden

  if (!expunge) {

    UPDATE linktable SET
      visibility = VISIBILITY_HIDDEN
    WHERE id1 = <id1> AND
      id2 = <id2> AND
      link_type = <link_type>;

  } else {

    DELETE FROM linktable
    WHERE id1 = <id1> AND
      id2 = <id2> AND
      link_type = <link_type>;
  }

Updating counttable

if (update_count_needed) {

  INSERT INTO counttable (
    id, id_type, assoc_type, count, time, version
  ) VALUES (
    <id1>, getId1Type(), <link_type>, 0, getSystemTime(), 0
  ) ON DUPLICATE KEY UPDATE id_type = getId1Type(),
    count = IF (count = 0, 0, count - 1),
    time = getSystemTime(),
    version = version + 1;

}

COMMIT

6. SQL for COUNT_LINKS(id1, link_type) operation

SELECT COUNT FROM counttable
WHERE id = <id1> AND
  link_type = <link_type>;

7. SQL for GET_LINK(id1, link_type, id2) operation

SELECT id1, id2, link_type, visibility, data, time, version FROM linktable
WHERE id1 = <id1> AND
  id2 = <id2> AND
  link_type = <link_type>;

8. SQL for GET_LINK_RANGE(id1, link_type, minTime, maxTime, offset, limit)
operation

SELECT id1, id2, link_type, visibility, data, time, version FROM linktable
WHERE id1 = <id1> AND
  link_type = <link_type> AND
  time >= <minTime> AND
  time <= <maxTimestamp> AND
  visibility = VISIBILITY_DEFAULT
ORDER BY time DESC LIMIT <offset>, <limit>;



