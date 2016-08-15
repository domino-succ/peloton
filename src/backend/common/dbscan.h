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
#include <unordered_map>
#include <map>
#include <sstream>
#include <fstream>

namespace peloton {

class Region {
 public:
  // Give two bitsets, just copy them. This is only used to create/set a cluster
  // region. So the 'cluster' passed by should always be true
  Region(Bitset &bitset)
      : bitset_(bitset), cluster_no_(0), x_scale_(0), y_scale_(0) {}

  Region(int wid_scale, std::vector<int> wids, int iid_scale,
         std::vector<int> iids)
      : cluster_no_(0), x_scale_(wid_scale), y_scale_(iid_scale) {
    // Set the bits
    SetCover(wid_scale, wids, iid_scale, iids);
  }

  Region() : cluster_no_(0), x_scale_(0), y_scale_(0) {}
  ~Region() {}

  // Just copy bitset, involves char* copy, but no deep copying here
  void SetCover(Region &rh_region) { bitset_ = rh_region.GetBitset(); }

  // Set two bitsets: the scale and bits
  void SetCover(int wid_scale, std::vector<int> &wids, int iid_scale,
                std::vector<int> &iids) {

    // Allocate bitset
    bitset_.Resize(wid_scale * iid_scale);

    // make sure wids and iids have the same size
    assert(wids.size() == iids.size());

    for (uint32_t i = 0; i < wids.size(); i++) {

      // Compute the bit
      int x = wids[i];
      int y = iids[i];

      int bit = LocateBit(x, y);

      // Set bit
      bitset_.Set(bit);
    }
  }

  // The overlap value is the multiply for wid and iid
  int OverlapValue(Region &rh_region) {
    // convert rh_region to RegionTpcc.
    return GetBitset().CountAnd(rh_region.GetBitset());
  }

  // Compute the overlay (OR operation) and return a new Region.
  // FIXME: make sure default copy is right (for return)
  Region Overlay(Region &rh_region) {
    Bitset overlay = GetBitset().OR(rh_region.GetBitset());

    // Return a cluster region
    return Region(overlay);
  }

  Bitset &GetBitset() { return bitset_; }

  void SetClusterNo(int cluster) { cluster_no_ = cluster; }
  void ClearClusterNo() { cluster_no_ = 0; }
  int GetClusterNo() { return cluster_no_; }

 private:
  // Note: x and y start from 0
  int LocateBit(int x, int y) { return x + (y * x_scale_); }

 private:
  // The vector expression for this region
  // std::vector<uint32_t> cover_;
  // For simplicity, only consider two conditions: wid and iid
  Bitset bitset_;

  // Cluster No.
  int cluster_no_;

  // For simplicity, we only support two dimension for now
  int x_scale_;
  int y_scale_;
};

class ClusterRegion : public Region {
 public:
  // This is only used to create cluster region. cluster_flag should be true,
  // when set the first txn to it
  ClusterRegion() : txn_count_(0), init_(false) {}

 public:
  int IsInit() { return init_; }
  void SetInit() { init_ = true; }

  void IncreaseMemberCount() { txn_count_++; }
  int GetMemberCount() { return txn_count_; }

 private:
  // If this region is a cluster, item_count_ is how many txns in the cluster
  int txn_count_;

  // whether has been set bitset
  bool init_;
};

class SingleRegion : public Region {
 public:
  SingleRegion(int wid_scale, std::vector<int> wids, int iid_scale,
               std::vector<int> iids, bool local, int warehouse)
      : Region(wid_scale, wids, iid_scale, iids),
        core_(false),
        noise_(false),
        sum_overlap_(0),
        marked_(false),
        local_(local),
        warehouse_id_(warehouse) {
    neighbor_region_.SetCover(*this);
    neighbor_region_.SetInit();
  }

  // TODO: SHOULD be deleted later. Just let ycsb pass compile
  SingleRegion()
      : core_(false),
        noise_(false),
        sum_overlap_(0),
        marked_(false),
        local_(true),
        warehouse_id_(-1) {}

 public:
  std::unordered_map<uint32_t, int> &GetNeighbors() { return neighbors_; }
  bool IsCore() { return core_; }
  void SetCore() { core_ = true; }

  bool IsNoise() { return noise_; }
  void SetNoise() { noise_ = true; }

  bool IsMarked() { return marked_; }
  void SetMarked() { marked_ = true; }

  bool IsLocal() { return local_; }
  void SetRemote() { local_ = false; }

  void SetWarehouseId(int id) { warehouse_id_ = id; }
  int GetWarehouseId() { return warehouse_id_; }

  void AddNeighbor(uint32_t region_idx, int overlap) {
    auto item = std::make_pair(region_idx, overlap);
    neighbors_.insert(item);

    sum_overlap_ = sum_overlap_ + overlap;
  }

  int RemoveNeighbor(uint32_t region_idx) {

    int overlap = neighbors_.find(region_idx)->second;

    sum_overlap_ = sum_overlap_ - overlap;

    return neighbors_.erase(region_idx);
  }

  std::unordered_map<uint32_t, int>::iterator RemoveNeighbor(
      std::unordered_map<uint32_t, int>::iterator region_idx) {

    int overlap = region_idx->second;

    sum_overlap_ = sum_overlap_ - overlap;

    return neighbors_.erase(region_idx);
  }

  int GetSumOverlap() { return sum_overlap_; }

  ClusterRegion &GetNeighborRegion() { return neighbor_region_; }

 private:
  // Neighbors of this region: region_index: overlap
  std::unordered_map<uint32_t, int> neighbors_;

  // neigbor_region_ is used to compute the overlap between two node's neighbors
  // For tpcc remote node only have one or two overlap? But normaly the overlap
  // for two node wthin the same warehouse is large. So there is a threshold, if
  // if the overlap between two neigbor_region_ is smaller than the threshold,
  // this node should not be considered when clustering
  ClusterRegion neighbor_region_;

  // Core
  bool core_;

  // Noise
  bool noise_;

  // sum overlap for all neighbors
  int sum_overlap_;

  // tag this region whether it has been processed
  bool marked_;

  // Only for test and analysis. Same with local warehouse or not
  bool local_;
  int warehouse_id_;
};

class DBScan {
 public:
  DBScan(std::vector<SingleRegion> &input_regions, int input_min_pts)
      : regions_(input_regions), minPts_(input_min_pts), cluster_count_(0) {}

  int Clustering() {
    // The number of all the regions
    int region_count = regions_.size();

    // Cluster tag starting from 1
    int cluster = 1;

    // Iterate all regions
    for (int region_index = 0; region_index < region_count; region_index++) {

      SingleRegion &region = regions_.at(region_index);

      // If the region is marked, continue to process the next region
      if (region.IsMarked()) continue;

      // Start a new cluster
      if (ExpandCluster(region, region_index, cluster)) {
        cluster++;
      }
    }

    // Clean the noise cluster
    ClearNoiseCluster(10);

    return cluster_count_;
  }

  // Iterate all original regions, find out the neighbors and compute the
  // overlap with the neighbors
  void PreProcess(int range) {
    // Get neighbors for each region to form a graph
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

          // Accumulate region
          Region overlay = regions_.at(region).GetNeighborRegion().Overlay(
              regions_.at(compare_region));
          // Set the new overlay
          regions_.at(region).GetNeighborRegion().SetCover(overlay);

          // Accumulate region for neighbor
          Region remote_overlay =
              regions_.at(compare_region).GetNeighborRegion().Overlay(
                  regions_.at(region));
          // Set the new overlay
          regions_.at(compare_region).GetNeighborRegion().SetCover(
              remote_overlay);
        }
      }
    }

    std::cout << "Finish neighbor compute, entering remote node analysis..."
              << std::endl;

    //////////////////////////////////////
    // Dominate overlap analysis
    //////////////////////////////////////

    // Dominate means a node and its neighbors. Dominate overlay is the union
    // region for this node and all its neighbors GetNeighborRegion().
    //
    // Computer the overlap for each node's dominate. Put each overlap in the
    // corresponding slot.
    //
    // A slot is a number range, which is 50 by default. So which slot a number
    // x falls in love is: x/range

    // Create number slot
    int remote = 0;
    std::map<int, std::vector<int>> slots;
    std::map<int, int> slots_count;
    int max_slot = -1;
    int max_count = 0;

    for (uint32_t region = 0; region < regions_.size() - 1; region++) {
      for (std::unordered_map<uint32_t, int>::iterator iter =
               regions_.at(region).GetNeighbors().begin();
           iter != regions_.at(region).GetNeighbors().end(); iter++) {

        // Get neighbor id
        int neighbor_idx = iter->first;

        // Compute the overlap
        int overlap = regions_.at(region).GetNeighborRegion().OverlapValue(
            regions_.at(neighbor_idx).GetNeighborRegion());

        if (overlap != 0) {

          int entry = overlap / range;

          slots[entry].push_back(overlap);
          slots_count[entry]++;

          //          // log file
          //          // Write LogTable into a file
          //          out << overlap << "\n";

          assert((uint32_t)slots_count[entry] == slots[entry].size());

          // Record the largest overlap
          if (slots_count[entry] > max_count) {
            max_count = slots_count[entry];
            max_slot = entry;
          }
        }
      }
    }

    std::stringstream oss;
    oss << "overlap";
    std::ofstream out(oss.str(), std::ofstream::out);

    for (auto &entry : slots_count) {
      // log file
      // Write LogTable into a file
      out << entry.first << " ";
      out << entry.second << "\n";
    }

    out.flush();
    out.close();

    std::cout << "max slot is : " << max_slot
              << " and max cout is : " << max_count << std::endl;

    std::cout << "The slot info: " << std::endl;
    for (std::map<int, int>::iterator iter = slots_count.begin();
         iter != slots_count.end(); iter++) {
      std::cout << iter->first << "---" << iter->second << std::endl;
    }

    // Now, we already get the number for all slots. Let's see which slot is
    // empty. Iterate the map from the largest slot
    for (std::map<int, std::vector<int>>::reverse_iterator iter =
             slots.rbegin();
         iter != slots.rend(); iter++) {

      if (iter->first != max_slot) {
        continue;
      }

      std::map<int, std::vector<int>>::reverse_iterator nxt =
          std::next(iter, 1);

      if (nxt == slots.rend()) {
        break;
      }

      int idx_next = nxt->first;

      // sort the overlap in this slot
      std::sort(slots[idx_next].begin(), slots[idx_next].end(),
                std::greater<int>());

      // pick the first overlap
      remote = slots[idx_next].front();
    }

    // For test
    std::map<int, int> slots_count2;

    std::cout << "entering checking..." << std::endl;
    for (uint32_t region = 0; region < regions_.size() - 1; region++) {

      for (std::unordered_map<uint32_t, int>::iterator iter =
               regions_.at(region).GetNeighbors().begin();
           iter != regions_.at(region).GetNeighbors().end(); iter++) {

        // Get neighbor id
        int neighbor_idx = iter->first;

        if (regions_.at(region).GetWarehouseId() !=
            regions_.at(neighbor_idx).GetWarehouseId()) {
          // Compute the overlap
          int overlap = regions_.at(region).GetNeighborRegion().OverlapValue(
              regions_.at(neighbor_idx).GetNeighborRegion());

          if (overlap != 0) {

            int entry = overlap / range;

            slots_count2[entry]++;
          }
        }
      }
    }
    std::stringstream oss2;
    oss2 << "remote";
    std::ofstream out2(oss2.str(), std::ofstream::out);

    for (auto &entry : slots_count2) {
      // log file
      // Write LogTable into a file
      out2 << entry.first << " ";
      out2 << entry.second << "\n";
    }

    out2.flush();
    out2.close();
    // end test

    std::cout << "Finish remote: , entering neighbor remove..." << remote
              << std::endl;
    // Delete the remote relationship (like the remote warehouse)
    // For each node (region), compute the overlap for:
    // neighbor_region_ with each neighbor's neighbor_region_
    // If the result (overlap) is smaller than the threshold, remove the
    // neighbor relationship
    for (uint32_t region = 0; region < regions_.size() - 1; region++) {

      for (std::unordered_map<uint32_t, int>::iterator iter =
               regions_.at(region).GetNeighbors().begin();
           iter != regions_.at(region).GetNeighbors().end();) {

        // Get neighbor id
        int neighbor_idx = iter->first;

        // Compute the overlap
        int overlap = regions_.at(region).GetNeighborRegion().OverlapValue(
            regions_.at(neighbor_idx).GetNeighborRegion());

        // Cut off the neighbor relationship.
        if (overlap <= remote) {
          iter = regions_.at(region).RemoveNeighbor(iter);
          regions_.at(neighbor_idx).RemoveNeighbor(region);

          // std::cout << "overlap: " << overlap << std::endl;
        } else {
          iter++;
        }

        //        // For test
        //        if (regions_.at(region).GetWarehouseId() !=
        //            regions_.at(neighbor_idx).GetWarehouseId()) {
        //
        //          std::cout << "overlap-----------------: " << overlap <<
        // std::endl;
        //        }
        //        // end test
      }
    }
  }

  bool ExpandCluster(SingleRegion &region, int region_idx, int cluster) {

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

      // Update this region into cluster_meta
      UpdateClusterMeta(region_idx, cluster);

      for (auto &neighbor : region.GetNeighbors()) {
        uint32_t idx = neighbor.first;
        SingleRegion &neighbor_region = regions_.at(idx);

        // If this region has been processed, skip it
        if (neighbor_region.IsMarked()) continue;

        // If the neighbor is also a core, expand it
        if (neighbor_region.GetSumOverlap() >= minPts_) {
          ExpandCluster(neighbor_region, idx, cluster);
        }
        // Just mark this node with cluster and update meta
        else {
          // Otherwise, mark the neighbor as the same cluster
          neighbor_region.SetClusterNo(cluster);
          neighbor_region.SetMarked();

          // Update this region into cluster_meta
          UpdateClusterMeta(idx, cluster);
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

    // For test
    int count = 0;

    // Iterate all txns
    for (auto &region : regions_) {
      // Get the cluster tag
      int cluster = region.GetClusterNo();

      // For test
      if (cluster == 7 || cluster == 8) {
        std::cout << "Region: " << count << " cluster: " << cluster
                  << std::endl;
      }
      count++;
      // end test

      // Skip the noise nodes (cluster starts from 1)
      if (cluster < 1) continue;

      // Compute the overall region for each cluster
      // Note: cluster tag starts from 1, so the index should be cluster-1

      // If the cluster already has txns, just compute the overlay
      if (clusters_.find(cluster) != clusters_.end()) {
        Region r = region.Overlay(clusters_[cluster]);
        clusters_[cluster].SetCover(r);
      }
      // Create
      else {
        ClusterRegion c_region;
        c_region.SetCover(region);
        clusters_.emplace(cluster, c_region);
        clusters_[cluster].SetInit();
      }

      // Increase the total txns in this cluster
      clusters_[cluster].IncreaseMemberCount();

      // Set the cluster NO. (We don't need this, but in order to keep
      // insistence)
      clusters_[cluster].SetClusterNo(cluster);
    }
  }

  std::unordered_map<int, ClusterRegion> &GetClusters() { return clusters_; }

  void DebugPrintRegion() {
    // The number of all the regions
    int region_count = regions_.size();

    // Print all regions
    for (int region_index = 0; region_index < region_count; region_index++) {
      SingleRegion &region = regions_.at(region_index);

      std::cout << "Region: " << region_index
                << "belongs to cluster: " << region.GetClusterNo()
                << ". Neighbors are" << region.GetNeighbors().size() << ": ";

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
    for (auto &cluster : clusters_) {

      std::cout << "Cluster: " << cluster.first
                << ". Its members are: " << cluster.second.GetMemberCount();

      std::cout << std::endl;
    }
  }

  void DebugPrintClusterMeta() {

    // Print all
    for (auto &cluster : cluster_meta_) {

      std::cout << "Cluster: " << cluster.first
                << ". Its members are: " << cluster.second.size();

      std::cout << std::endl;
    }
  }

 private:
  void UpdateClusterMeta(int region_idx, int cluster) {
    // Insert this region into cluster_meta
    std::unordered_map<int, std::vector<int>>::iterator iter =
        cluster_meta_.find(cluster);

    // Increase
    if (iter != cluster_meta_.end()) {
      iter->second.push_back(region_idx);
    }
    // Create
    else {
      cluster_meta_.emplace(cluster, std::vector<int>(1, region_idx));
    }
  }

  void ClearNoiseCluster(int noise) {
    for (std::unordered_map<int, std::vector<int>>::iterator iter =
             cluster_meta_.begin();
         iter != cluster_meta_.end();) {

      // Handle noise cluster
      if (iter->second.size() < (uint32_t)noise) {
        // remove cluster tag from each region
        for (auto &region_idx : iter->second) {
          regions_.at(region_idx).ClearClusterNo();

          // Test
          int c = regions_.at(region_idx).GetClusterNo();
          std::cout << "remove cluster " << iter->first << ": region "
                    << region_idx << "After remove cluster No. is: " << c
                    << std::endl;
        }
        // remove this cluster
        iter = cluster_meta_.erase(iter);
      } else {
        iter++;
      }
    }

    cluster_count_ = cluster_meta_.size();
  }

 private:
  std::vector<SingleRegion> regions_;
  int minPts_;

  int cluster_count_;
  // std::vector<ClusterRegion> clusters_;
  std::unordered_map<int, ClusterRegion> clusters_;

  // For example: cluster1: <region2, region3, region19....>
  std::unordered_map<int, std::vector<int>> cluster_meta_;
};

}  // end namespace peloton
