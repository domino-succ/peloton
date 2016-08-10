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
#include "backend/common/bitset.h"

#include <stdint.h>
#include <vector>
#include <boost/numeric/ublas/matrix.hpp>

namespace peloton {

class Region {
 public:
  // Give two bitsets, just copy them. This is only used to create/set a cluster
  // region. So the 'cluster' passed by should always be true
  Region(Bitset &wid, Bitset &iid, bool cluster)
      : wid_bitset_(wid),
        iid_bitset_(iid),
        core_(false),
        noise_(false),
        sum_overlap_(0),
        cluster_no_(0),
        cluster_flag_(cluster),
        marked_(false),
        txn_count_(0) {}

  Region(int wid_scale, std::vector<int> wids, int iid_scale,
         std::vector<int> iids)
      : core_(false),
        noise_(false),
        sum_overlap_(0),
        cluster_no_(0),
        cluster_flag_(false),
        marked_(false),
        txn_count_(0) {
    // Resize bitset
    wid_bitset_.Resize(wid_scale);
    iid_bitset_.Resize(iid_scale);

    // Set bit
    wid_bitset_.Set(wids);
    iid_bitset_.Set(iids);
  }

  // This is only used to create cluster region. cluster_flag should be true,
  // when set the first txn to it
  Region()
      : core_(false),
        noise_(false),
        sum_overlap_(0),
        cluster_no_(0),
        cluster_flag_(false),
        marked_(false),
        txn_count_(0) {}

  ~Region() {}

  // Just copy bitset, involves char* copy, but no deep copying here
  void SetCover(Region &rh_region) {
    wid_bitset_ = rh_region.GetWid();
    iid_bitset_ = rh_region.GetIid();
  }

  // Set two bitsets: the scale and bits
  void SetCover(int wid_scale, std::vector<int> wids, int iid_scale,
                std::vector<int> iids) {
    // Resize bitset
    wid_bitset_.Resize(wid_scale);
    iid_bitset_.Resize(iid_scale);

    // Set bit
    wid_bitset_.Set(wids);
    iid_bitset_.Set(iids);
  }

  std::vector<std::pair<uint32_t, int>> &GetNeighbors() { return neighbors_; }
  bool IsCore() { return core_; }
  void SetCore() { core_ = true; }

  bool IsNoise() { return noise_; }
  void SetNoise() { noise_ = true; }

  bool IsMarked() { return marked_; }
  void SetMarked() { marked_ = true; }

  // The overlap value is the multiply for wid and iid
  int OverlapValue(Region &rh_region) {
    // convert rh_region to RegionTpcc. We should refactor this later
    int wid_overlap = GetWid().CountAnd(rh_region.GetWid());
    int iid_overlap = GetIid().CountAnd(rh_region.GetIid());

    return wid_overlap * iid_overlap;
  }

  // Compute the overlay (OR operation) and return a new Region.
  // FIXME: make sure default copy is right (for return)
  Region Overlay(Region &rh_region) {
    Bitset wid_overlay = GetWid().OR(rh_region.GetWid());
    Bitset iid_overlay = GetIid().OR(rh_region.GetIid());

    // Return a cluster region
    return Region(wid_overlay, iid_overlay, true);
  }

  Bitset &GetWid() { return wid_bitset_; }
  Bitset &GetIid() { return iid_bitset_; }

  void AddNeighbor(uint32_t region_idx, int overlap) {
    auto item = std::make_pair(region_idx, overlap);
    neighbors_.push_back(item);

    sum_overlap_ = sum_overlap_ + overlap;
  }

  // Only used for cluster
  void SetClusterNo(int cluster) { cluster_no_ = cluster; }
  int GetClusterNo() { return cluster_no_; }

  void SetClusterFlag(bool b_cluster) { cluster_flag_ = b_cluster; }
  int IsCluster() { return cluster_flag_; }

  int GetSumOverlap() { return sum_overlap_; }

  void IncreaseMemberCount() { txn_count_++; }
  int GetMemberCount() { return txn_count_; }

 private:
  // The vector expression for this region
  // std::vector<uint32_t> cover_;
  // For simplicity, only consider two conditions: wid and iid
  Bitset wid_bitset_;
  Bitset iid_bitset_;

  // Neighbors of this region: region_index: overlap
  std::vector<std::pair<uint32_t, int>> neighbors_;

  // Core
  bool core_;

  // Noise
  bool noise_;

  // sum overlap for all neighbors
  int sum_overlap_;

  // Cluster No.
  int cluster_no_;

  // Cluster flag to show whether it is a cluster or a normal region (query)
  bool cluster_flag_;

  // tag this region whether it has been processed
  bool marked_;

  // If this region is a cluster, item_count_ is how many txns in the cluster
  int txn_count_;
};

class DBScan {
 public:
  DBScan(std::vector<Region> &input_regions, int input_min_pts)
      : regions_(input_regions), minPts_(input_min_pts), cluster_count_(0) {}

  int Clustering() {
    // The number of all the regions
    int region_count = regions_.size();

    // Cluster tag starting from 1
    int cluster = 1;

    // Iterate all regions
    for (int region_index = 0; region_index < region_count; region_index++) {

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

      for (uint32_t compare_region = region + 1;
           compare_region < regions_.size(); compare_region++) {
        // Computer the overlap
        int overlap =
            regions_.at(region).OverlapValue(regions_.at(compare_region));

        // If overlap !=0, these two regions should be neighbors for each other
        if (overlap != 0) {
          regions_.at(region).AddNeighbor(compare_region, overlap);
          regions_.at(compare_region).AddNeighbor(region, overlap);

          //          // for test
          //          std::cout << "Region" << region << ": Neighbor" <<
          // compare_region
          //                    << "--Overlap: " << overlap << std::endl;
          //          // end test
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
      region.SetClusterNo(cluster);
      region.SetMarked();

      for (auto &neighbor : region.GetNeighbors()) {
        uint32_t idx = neighbor.first;
        Region &neighbor_region = regions_.at(idx);

        // If this region has been processed, skip it
        if (neighbor_region.IsMarked()) continue;

        // Otherwise, mark the neighbor as the same cluster
        neighbor_region.SetClusterNo(cluster);
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
  // After Expand, we already get the cluster for each region.
  // This is for further cluster analysis and must be used after Expand
  // execution.
  void SetClusterRegion() {
    // First create the regions corresponding to the clusters
    clusters_.resize(cluster_count_);

    // Iterate all txns
    for (auto &region : regions_) {
      // Get the cluster tag
      int cluster = region.GetClusterNo();

      // Skip the noise nodes (cluster starts from 1)
      if (cluster < 1) continue;

      // Compute the overall region for each cluster
      // Note: cluster tag starts from 1, so the index should be cluster-1

      // If the cluster already has txns, just compute the overlay
      if (clusters_[cluster - 1].IsCluster()) {
        Region r = region.Overlay(clusters_[cluster - 1]);
        clusters_[cluster - 1].SetCover(r);
      }
      // If the corresponding cluster has no txns yet, first should set
      // the region to it
      else {
        clusters_[cluster - 1].SetCover(region);
        clusters_[cluster - 1].SetClusterFlag(true);
      }

      // Increase the total txns in this cluster
      clusters_[cluster - 1].IncreaseMemberCount();

      // Set the cluster NO. (We don't need this, but in order to keep
      // insistence)
      clusters_[cluster - 1].SetClusterNo(cluster);
    }
  }

  std::vector<Region> &GetClusters() { return clusters_; }

  void DebugPrintRegion() {
    // The number of all the regions
    int region_count = regions_.size();

    // Print all regions
    for (int region_index = 0; region_index < region_count; region_index++) {
      Region &region = regions_.at(region_index);

      std::cout << "Region: " << region_index
                << "belongs to cluster: " << region.GetClusterNo()
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
      Region &region = clusters_.at(cluster_index);

      std::cout << "Cluster: " << cluster_index
                << ". Its members are: " << region.GetMemberCount();

      std::cout << std::endl;
    }
  }

 private:
  std::vector<Region> regions_;
  int minPts_;

  int cluster_count_;
  std::vector<Region> clusters_;
};

}  // end namespace peloton
