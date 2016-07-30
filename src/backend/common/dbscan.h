//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager.h
//
// Identification: src/backend/concurrency/transaction_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/logger.h"

#include <stdint.h>
#include <vector>
#include <boost/numeric/ublas/matrix.hpp>

namespace peloton {

class Region {
 public:
  Region(std::vector<uint32_t> &cover)
      : cover_(cover),
        core_(false),
        noise_(false),
        sum_overlap_(0),
        cluster_(0),
        marked_(false),
        item_count_(0) {}
  Region()
      : core_(false),
        noise_(false),
        sum_overlap_(0),
        cluster_(0),
        marked_(false),
        item_count_(0) {}
  ~Region() {}

  void SetCover(std::vector<uint32_t> cover) { cover_ = cover; }

  std::vector<uint32_t> &GetCover() { return cover_; }
  std::vector<std::pair<uint32_t, int>> &GetNeighbors() { return neighbors_; }
  bool IsCore() { return core_; }
  void SetCore() { core_ = true; }

  bool IsNoise() { return noise_; }
  void SetNoise() { noise_ = true; }

  bool IsMarked() { return marked_; }
  void SetMarked() { marked_ = true; }

  // The two vector should have the same size.
  std::vector<uint32_t> Overlap(Region &rh_region) {
    assert(cover_.size() == rh_region.GetCover().size());
    std::vector<uint32_t> intersect(cover_.size());

    for (uint32_t idx = 0; idx < cover_.size(); idx++) {
      if (cover_.at(idx) * rh_region.GetCover().at(idx) != 0) {
        intersect.at(idx) = cover_.at(idx) + rh_region.GetCover().at(idx);
      } else {
        intersect.at(idx) = 0;
      }
    }

    // In c++11, the efficient way to return a vector is std::vector vect = f();
    return intersect;
  }

  int OverlapValue(Region &rh_region) {
    assert(cover_.size() == rh_region.GetCover().size());
    int intersect = 0;

    for (uint32_t idx = 0; idx < cover_.size(); idx++) {
      if (cover_.at(idx) * rh_region.GetCover().at(idx) != 0) {
        intersect = intersect + (cover_.at(idx) + rh_region.GetCover().at(idx));
      }
    }

    return intersect;
  }

  std::vector<uint32_t> Overlay(Region &rh_region) {
    assert(cover_.size() == rh_region.GetCover().size());
    std::vector<uint32_t> all_cover(cover_.size());

    for (uint32_t idx = 0; idx < cover_.size(); idx++) {
      all_cover.at(idx) = cover_.at(idx) + rh_region.GetCover().at(idx);
    }

    // In c++11, the efficient way to return a vector is std::vector vect = f();
    return all_cover;
  }

  void AddNeighbor(uint32_t region_idx, int overlap) {
    auto item = std::make_pair(region_idx, overlap);
    neighbors_.push_back(item);

    sum_overlap_ = sum_overlap_ + overlap;
  }

  void SetCluster(int cluster) { cluster_ = cluster; }
  int GetCluster() { return cluster_; }

  int GetSumOverlap() { return sum_overlap_; }

  void IncreaseMemberCount() { item_count_++; }
  int GetMemberCount() { return item_count_; }

 private:
  // The vector expression for this region
  std::vector<uint32_t> cover_;

  // Neighbors of this region: region_index: overlap
  std::vector<std::pair<uint32_t, int>> neighbors_;

  // Core
  bool core_;

  // Noise
  bool noise_;

  // sum overlap for all neighbors
  int sum_overlap_;

  // Cluster tag
  int cluster_;

  // tag this region whether it has been processed
  bool marked_;

  // If this region is a cluster, item_count_ is how many txns in the cluster
  int item_count_;
};

class DBScan {
 public:
  DBScan(std::vector<Region> &input_regions, int input_min_pts)
      : regions_(input_regions), minPts_(input_min_pts), cluster_count_(0) {
    // The scale of the vector
    scale_ = regions_.front().GetCover().size();
  }

  int Clustering() {
    // The number of all the regions
    int region_count = regions_.size();

    // Cluster tag starting from 1
    int cluster = 1;

    // Iterate all regions
    for (int region_index = 0; region_index < region_count; region_index++) {

      std::cout << "THe current cluster is: " << cluster << std::endl;

      Region &region = regions_.at(region_index);

      // If the region is marked, continue to process the next region
      if (region.IsMarked()) continue;

      // Start a new cluster
      if (ExpandCluster(region, cluster)) {
        cluster++;
      }
    }

    cluster_count_ = cluster - 1;
    return cluster_count_;
  }

  // Iterate all original regions, find out the neighbors and compute the
  // overlap with the neighbors
  void PreProcess() {
    // Iterate all regions
    for (uint32_t region = 0; region < regions_.size() - 1; region++) {
      // For a region, iterate each region from the beginning of the original
      // region list
      // If their is overlap between them, put the current region into neighbors
      // we only need iterate one time
      LOG_INFO("This is the txn: %d", region);
      for (uint32_t compare_region = region + 1;
           compare_region < regions_.size(); compare_region++) {
        // Computer the overlap
        int overlap =
            regions_.at(region).OverlapValue(regions_.at(compare_region));

        // If overlap !=0, these two regions should be neighbors for each other
        if (overlap != 0) {
          regions_.at(region).AddNeighbor(compare_region, overlap);
          regions_.at(compare_region).AddNeighbor(region, overlap);
        }
      }
    }
  }

  bool ExpandCluster(Region &region, int cluster) {

    if (region.GetSumOverlap() < minPts_) {

      // this point is noise
      // output[p] = -1;

      region.SetNoise();
      region.SetMarked();
      return false;

    } else {

      region.SetCore();
      region.SetCluster(cluster);
      region.SetMarked();

      for (auto &neighbor : region.GetNeighbors()) {
        uint32_t idx = neighbor.first;
        Region &neighbor_region = regions_.at(idx);

        // If this region has been processed, skip it
        if (neighbor_region.IsMarked()) continue;

        // Otherwise, mark the neighbor as the same cluster
        neighbor_region.SetCluster(cluster);
        neighbor_region.SetMarked();

        // If the neighbor is also a core, expand it
        if (neighbor_region.GetSumOverlap() >= minPts_) {
          ExpandCluster(neighbor_region, cluster);
        }
      }
    }

    return true;
  }

  // Iterate all regions, see which cluster this region belongs to
  // Perform union operation between this region and the cluster
  void SetClusterRegion() {
    // First create the regions corresponding to the clusters
    clusters_region_.resize(cluster_count_);

    std::cout << "the scale of the vector is %d" << scale_ << std::endl;

    // Init each cluster_region
    std::vector<uint32_t> init_cover(scale_, 0);

    for (auto &cluster_region : clusters_region_) {
      cluster_region.SetCover(init_cover);
    }

    // Iterate all txns
    for (auto &region : regions_) {
      // Get the cluster tag
      int cluster = region.GetCluster();

      // Skip the noise nodes
      if (cluster < 1) continue;

      // Compute the overall region for each cluster
      // Note: cluster tag starts from 1, so the index should be cluster-1
      clusters_region_[cluster - 1]
          .SetCover(region.Overlay(clusters_region_[cluster - 1]));

      // Increase the total txns in this cluster
      clusters_region_[cluster - 1].IncreaseMemberCount();

      // Set the cluster NO. (We don't need this, but in order to keep
      // insistence)
      clusters_region_[cluster - 1].SetCluster(cluster);
    }
  }

  std::vector<Region> &GetClustersRegion() { return clusters_region_; }

  void DebugPrintRegion() {
    // The number of all the regions
    int region_count = regions_.size();

    // Print all regions
    for (int region_index = 0; region_index < region_count; region_index++) {
      Region &region = regions_.at(region_index);

      std::cout << "Region: " << region_index
                << "belongs to cluster: " << region.GetCluster()
                << ". Its neighbors are: ";

      for (auto &neighbor : region.GetNeighbors()) {
        uint32_t idx = neighbor.first;
        int overlap = neighbor.second;

        std::cout << idx << ":" << overlap << ",";
      }
      std::cout << std::endl;
    }
  }

  void DebugPrintCluster() {

    // Print all
    for (int cluster_index = 0; cluster_index < cluster_count_;
         cluster_index++) {
      Region &region = clusters_region_.at(cluster_index);

      std::cout << "Cluster: " << cluster_index
                << ". Its members are: " << region.GetMemberCount();

      std::cout << std::endl;
    }
  }

 private:
  std::vector<Region> regions_;
  int scale_;
  std::vector<int> labels_;
  int minPts_;

  int cluster_count_;
  std::vector<Region> clusters_region_;
};

}  // end namespace peloton
