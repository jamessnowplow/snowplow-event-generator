/*
 * Copyright (c) 2021-2025 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.eventgen

import com.snowplowanalytics.snowplow.eventgen.protocol.common.ClusterAlgorithm
import org.specs2.mutable.Specification

class PartitionSpec extends Specification {

  "Partitioned generation" should {

    "produce disjoint user sets across partitions" in {
      val numUsers = 1000L

      val partitions = (0 until 3).map(i => GenConfig.Partition(instance = i, totalInstances = 3))
      val gens = partitions.map(p =>
        UserDistribution.selectPartitioned(numUsers, p, GenConfig.UserGraph.Distribution.Uniform)
      )

      val sampleSets = gens.map(g => (1 to 100).map(_ => g.sample.get).toSet)

      // No overlap between any pair of partitions
      for {
        i <- 0 until 3
        j <- (i + 1) until 3
      } yield (sampleSets(i) & sampleSets(j)) must beEmpty

      ok
    }

    "share identifiers across partitions for clustered users" in {
      val numUsers = 1000L
      val sharedIdentifierRate = 0.5
      val usersPerCluster = 10

      // Find a cluster with members in different partitions
      val clusterMembers = (0L until numUsers)
        .filter(ClusterAlgorithm.isInCluster(_, sharedIdentifierRate))
        .flatMap(id => ClusterAlgorithm.getClusterId(id, numUsers, sharedIdentifierRate, usersPerCluster).map(_ -> id))
        .groupBy(_._1).view.mapValues(_.map(_._2).toSeq).toMap

      val crossPartitionCluster = clusterMembers.values.find { members =>
        members.map(_ % 3).toSet.size > 1
      }

      crossPartitionCluster must beSome

      crossPartitionCluster.foreach { members =>
        val member1 = members.find(_ % 3 == 0).get
        val member2 = members.find(_ % 3 != 0).get

        val cookies1 = ClusterAlgorithm.generateIdentifiers(member1, "cookie", 3, numUsers, sharedIdentifierRate, usersPerCluster, 1).toSet
        val cookies2 = ClusterAlgorithm.generateIdentifiers(member2, "cookie", 3, numUsers, sharedIdentifierRate, usersPerCluster, 1).toSet

        (cookies1 & cookies2).exists(_.startsWith("shared_")) must beTrue
      }

      ok
    }
  }
}
