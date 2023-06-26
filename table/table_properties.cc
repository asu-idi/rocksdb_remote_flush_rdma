//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/table_properties.h"

#include <utility>

#include "db/seqno_to_time_mapping.h"
#include "memory/shared_package.h"
#include "port/malloc.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/unique_id.h"
#include "table/table_properties_internal.h"
#include "table/unique_id_impl.h"
#include "util/random.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

const uint32_t TablePropertiesCollectorFactory::Context::kUnknownColumnFamily =
    std::numeric_limits<int32_t>::max();

namespace {
void AppendProperty(std::string& props, const std::string& key,
                    const std::string& value, const std::string& prop_delim,
                    const std::string& kv_delim) {
  props.append(key);
  props.append(kv_delim);
  props.append(value);
  props.append(prop_delim);
}

template <class TValue>
void AppendProperty(std::string& props, const std::string& key,
                    const TValue& value, const std::string& prop_delim,
                    const std::string& kv_delim) {
  AppendProperty(props, key, std::to_string(value), prop_delim, kv_delim);
}
}  // namespace

std::string TableProperties::ToString(const std::string& prop_delim,
                                      const std::string& kv_delim) const {
  std::string result;
  result.reserve(1024);

  // Basic Info
  AppendProperty(result, "# data blocks", num_data_blocks, prop_delim,
                 kv_delim);
  AppendProperty(result, "# entries", num_entries, prop_delim, kv_delim);
  AppendProperty(result, "# deletions", num_deletions, prop_delim, kv_delim);
  AppendProperty(result, "# merge operands", num_merge_operands, prop_delim,
                 kv_delim);
  AppendProperty(result, "# range deletions", num_range_deletions, prop_delim,
                 kv_delim);

  AppendProperty(result, "raw key size", raw_key_size, prop_delim, kv_delim);
  AppendProperty(result, "raw average key size",
                 num_entries != 0 ? 1.0 * raw_key_size / num_entries : 0.0,
                 prop_delim, kv_delim);
  AppendProperty(result, "raw value size", raw_value_size, prop_delim,
                 kv_delim);
  AppendProperty(result, "raw average value size",
                 num_entries != 0 ? 1.0 * raw_value_size / num_entries : 0.0,
                 prop_delim, kv_delim);

  AppendProperty(result, "data block size", data_size, prop_delim, kv_delim);
  char index_block_size_str[80];
  snprintf(index_block_size_str, sizeof(index_block_size_str),
           "index block size (user-key? %d, delta-value? %d)",
           static_cast<int>(index_key_is_user_key),
           static_cast<int>(index_value_is_delta_encoded));
  AppendProperty(result, index_block_size_str, index_size, prop_delim,
                 kv_delim);
  if (index_partitions != 0) {
    AppendProperty(result, "# index partitions", index_partitions, prop_delim,
                   kv_delim);
    AppendProperty(result, "top-level index size", top_level_index_size,
                   prop_delim, kv_delim);
  }
  AppendProperty(result, "filter block size", filter_size, prop_delim,
                 kv_delim);
  AppendProperty(result, "# entries for filter", num_filter_entries, prop_delim,
                 kv_delim);
  AppendProperty(result, "(estimated) table size",
                 data_size + index_size + filter_size, prop_delim, kv_delim);

  AppendProperty(
      result, "filter policy name",
      filter_policy_name.empty() ? std::string("N/A") : filter_policy_name,
      prop_delim, kv_delim);

  AppendProperty(result, "prefix extractor name",
                 prefix_extractor_name.empty() ? std::string("N/A")
                                               : prefix_extractor_name,
                 prop_delim, kv_delim);

  AppendProperty(result, "column family ID",
                 column_family_id ==
                         ROCKSDB_NAMESPACE::TablePropertiesCollectorFactory::
                             Context::kUnknownColumnFamily
                     ? std::string("N/A")
                     : std::to_string(column_family_id),
                 prop_delim, kv_delim);
  AppendProperty(
      result, "column family name",
      column_family_name.empty() ? std::string("N/A") : column_family_name,
      prop_delim, kv_delim);

  AppendProperty(result, "comparator name",
                 comparator_name.empty() ? std::string("N/A") : comparator_name,
                 prop_delim, kv_delim);

  AppendProperty(
      result, "merge operator name",
      merge_operator_name.empty() ? std::string("N/A") : merge_operator_name,
      prop_delim, kv_delim);

  AppendProperty(result, "property collectors names",
                 property_collectors_names.empty() ? std::string("N/A")
                                                   : property_collectors_names,
                 prop_delim, kv_delim);

  AppendProperty(
      result, "SST file compression algo",
      compression_name.empty() ? std::string("N/A") : compression_name,
      prop_delim, kv_delim);

  AppendProperty(
      result, "SST file compression options",
      compression_options.empty() ? std::string("N/A") : compression_options,
      prop_delim, kv_delim);

  AppendProperty(result, "creation time", creation_time, prop_delim, kv_delim);

  AppendProperty(result, "time stamp of earliest key", oldest_key_time,
                 prop_delim, kv_delim);

  AppendProperty(result, "file creation time", file_creation_time, prop_delim,
                 kv_delim);

  AppendProperty(result, "slow compression estimated data size",
                 slow_compression_estimated_data_size, prop_delim, kv_delim);
  AppendProperty(result, "fast compression estimated data size",
                 fast_compression_estimated_data_size, prop_delim, kv_delim);

  // DB identity and DB session ID
  AppendProperty(result, "DB identity", db_id, prop_delim, kv_delim);
  AppendProperty(result, "DB session identity", db_session_id, prop_delim,
                 kv_delim);
  AppendProperty(result, "DB host id", db_host_id, prop_delim, kv_delim);
  AppendProperty(result, "original file number", orig_file_number, prop_delim,
                 kv_delim);

  // Unique ID, when available
  std::string id;
  Status s = GetUniqueIdFromTableProperties(*this, &id);
  AppendProperty(result, "unique ID",
                 s.ok() ? UniqueIdToHumanString(id) : "N/A", prop_delim,
                 kv_delim);

  SeqnoToTimeMapping seq_time_mapping;
  s = seq_time_mapping.Add(seqno_to_time_mapping);
  AppendProperty(result, "Sequence number to time mapping",
                 s.ok() ? seq_time_mapping.ToHumanString() : "N/A", prop_delim,
                 kv_delim);

  return result;
}

void TableProperties::Add(const TableProperties& tp) {
  data_size += tp.data_size;
  index_size += tp.index_size;
  index_partitions += tp.index_partitions;
  top_level_index_size += tp.top_level_index_size;
  index_key_is_user_key += tp.index_key_is_user_key;
  index_value_is_delta_encoded += tp.index_value_is_delta_encoded;
  filter_size += tp.filter_size;
  raw_key_size += tp.raw_key_size;
  raw_value_size += tp.raw_value_size;
  num_data_blocks += tp.num_data_blocks;
  num_entries += tp.num_entries;
  num_filter_entries += tp.num_filter_entries;
  num_deletions += tp.num_deletions;
  num_merge_operands += tp.num_merge_operands;
  num_range_deletions += tp.num_range_deletions;
  slow_compression_estimated_data_size +=
      tp.slow_compression_estimated_data_size;
  fast_compression_estimated_data_size +=
      tp.fast_compression_estimated_data_size;
}

std::map<std::string, uint64_t>
TableProperties::GetAggregatablePropertiesAsMap() const {
  std::map<std::string, uint64_t> rv;
  rv["data_size"] = data_size;
  rv["index_size"] = index_size;
  rv["index_partitions"] = index_partitions;
  rv["top_level_index_size"] = top_level_index_size;
  rv["filter_size"] = filter_size;
  rv["raw_key_size"] = raw_key_size;
  rv["raw_value_size"] = raw_value_size;
  rv["num_data_blocks"] = num_data_blocks;
  rv["num_entries"] = num_entries;
  rv["num_filter_entries"] = num_filter_entries;
  rv["num_deletions"] = num_deletions;
  rv["num_merge_operands"] = num_merge_operands;
  rv["num_range_deletions"] = num_range_deletions;
  rv["slow_compression_estimated_data_size"] =
      slow_compression_estimated_data_size;
  rv["fast_compression_estimated_data_size"] =
      fast_compression_estimated_data_size;
  return rv;
}

// WARNING: manual update to this function is needed
// whenever a new string property is added to TableProperties
// to reduce approximation error.
//
// TODO: eliminate the need of manually updating this function
// for new string properties
std::size_t TableProperties::ApproximateMemoryUsage() const {
  std::size_t usage = 0;
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
  usage += malloc_usable_size((void*)this);
#else
  usage += sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE

  std::size_t string_props_mem_usage =
      db_id.size() + db_session_id.size() + db_host_id.size() +
      column_family_name.size() + filter_policy_name.size() +
      comparator_name.size() + merge_operator_name.size() +
      prefix_extractor_name.size() + property_collectors_names.size() +
      compression_name.size() + compression_options.size();
  usage += string_props_mem_usage;

  for (auto iter = user_collected_properties.begin();
       iter != user_collected_properties.end(); ++iter) {
    usage += (iter->first.size() + iter->second.size());
  }

  return usage;
}

const std::string TablePropertiesNames::kDbId = "rocksdb.creating.db.identity";
const std::string TablePropertiesNames::kDbSessionId =
    "rocksdb.creating.session.identity";
const std::string TablePropertiesNames::kDbHostId =
    "rocksdb.creating.host.identity";
const std::string TablePropertiesNames::kOriginalFileNumber =
    "rocksdb.original.file.number";
const std::string TablePropertiesNames::kDataSize = "rocksdb.data.size";
const std::string TablePropertiesNames::kIndexSize = "rocksdb.index.size";
const std::string TablePropertiesNames::kIndexPartitions =
    "rocksdb.index.partitions";
const std::string TablePropertiesNames::kTopLevelIndexSize =
    "rocksdb.top-level.index.size";
const std::string TablePropertiesNames::kIndexKeyIsUserKey =
    "rocksdb.index.key.is.user.key";
const std::string TablePropertiesNames::kIndexValueIsDeltaEncoded =
    "rocksdb.index.value.is.delta.encoded";
const std::string TablePropertiesNames::kFilterSize = "rocksdb.filter.size";
const std::string TablePropertiesNames::kRawKeySize = "rocksdb.raw.key.size";
const std::string TablePropertiesNames::kRawValueSize =
    "rocksdb.raw.value.size";
const std::string TablePropertiesNames::kNumDataBlocks =
    "rocksdb.num.data.blocks";
const std::string TablePropertiesNames::kNumEntries = "rocksdb.num.entries";
const std::string TablePropertiesNames::kNumFilterEntries =
    "rocksdb.num.filter_entries";
const std::string TablePropertiesNames::kDeletedKeys = "rocksdb.deleted.keys";
const std::string TablePropertiesNames::kMergeOperands =
    "rocksdb.merge.operands";
const std::string TablePropertiesNames::kNumRangeDeletions =
    "rocksdb.num.range-deletions";
const std::string TablePropertiesNames::kFilterPolicy = "rocksdb.filter.policy";
const std::string TablePropertiesNames::kFormatVersion =
    "rocksdb.format.version";
const std::string TablePropertiesNames::kFixedKeyLen =
    "rocksdb.fixed.key.length";
const std::string TablePropertiesNames::kColumnFamilyId =
    "rocksdb.column.family.id";
const std::string TablePropertiesNames::kColumnFamilyName =
    "rocksdb.column.family.name";
const std::string TablePropertiesNames::kComparator = "rocksdb.comparator";
const std::string TablePropertiesNames::kMergeOperator =
    "rocksdb.merge.operator";
const std::string TablePropertiesNames::kPrefixExtractorName =
    "rocksdb.prefix.extractor.name";
const std::string TablePropertiesNames::kPropertyCollectors =
    "rocksdb.property.collectors";
const std::string TablePropertiesNames::kCompression = "rocksdb.compression";
const std::string TablePropertiesNames::kCompressionOptions =
    "rocksdb.compression_options";
const std::string TablePropertiesNames::kCreationTime = "rocksdb.creation.time";
const std::string TablePropertiesNames::kOldestKeyTime =
    "rocksdb.oldest.key.time";
const std::string TablePropertiesNames::kFileCreationTime =
    "rocksdb.file.creation.time";
const std::string TablePropertiesNames::kSlowCompressionEstimatedDataSize =
    "rocksdb.sample_for_compression.slow.data.size";
const std::string TablePropertiesNames::kFastCompressionEstimatedDataSize =
    "rocksdb.sample_for_compression.fast.data.size";
const std::string TablePropertiesNames::kSequenceNumberTimeMapping =
    "rocksdb.seqno.time.map";

#ifndef NDEBUG
// WARNING: TEST_SetRandomTableProperties assumes the following layout of
// TableProperties
//
// struct TableProperties {
//    int64_t orig_file_number = 0;
//    ...
//    ... int64_t properties only
//    ...
//    std::string db_id;
//    ...
//    ... std::string properties only
//    ...
//    std::string compression_options;
//    UserCollectedProperties user_collected_properties;
//    ...
//    ... Other extra properties: non-int64_t/non-std::string properties only
//    ...
// }
void TEST_SetRandomTableProperties(TableProperties* props) {
  Random* r = Random::GetTLSInstance();
  uint64_t* pu = &props->orig_file_number;
  assert(static_cast<void*>(pu) == static_cast<void*>(props));
  std::string* ps = &props->db_id;
  const uint64_t* const pu_end = reinterpret_cast<const uint64_t*>(ps);
  // Use the last string property's address instead of
  // the first extra property (e.g `user_collected_properties`)'s address
  // in the for-loop to avoid advancing pointer to pointing to
  // potential non-zero padding bytes between these two addresses due to
  // user_collected_properties's alignment requirement
  const std::string* const ps_end_inclusive = &props->compression_options;

  for (; pu < pu_end; ++pu) {
    *pu = r->Next64();
  }
  assert(static_cast<void*>(pu) == static_cast<void*>(ps));
  for (; ps <= ps_end_inclusive; ++ps) {
    *ps = r->RandomBinaryString(13);
  }
}
#endif

bool TableProperties::CHECKShared() { return is_shared_ && is_packaged_; }
void TableProperties::Pack() {
  if (!string_package_.empty() || !user_collected_properties_package_.empty() ||
      !readable_properties_package_.empty() || is_packaged_) {
    LOG("TableProperties has been packed, can't pack again.");
    return;
  }
  string_package_.push_back(
      std::make_pair(shm_package::Pack(db_id), db_id.length()));
  string_package_.push_back(
      std::make_pair(shm_package::Pack(db_session_id), db_session_id.length()));
  string_package_.push_back(
      std::make_pair(shm_package::Pack(db_host_id), db_host_id.length()));
  string_package_.push_back(std::make_pair(
      shm_package::Pack(column_family_name), column_family_name.length()));
  string_package_.push_back(std::make_pair(
      shm_package::Pack(filter_policy_name), filter_policy_name.length()));
  string_package_.push_back(std::make_pair(shm_package::Pack(comparator_name),
                                           comparator_name.length()));
  string_package_.push_back(std::make_pair(
      shm_package::Pack(merge_operator_name), merge_operator_name.length()));
  string_package_.push_back(
      std::make_pair(shm_package::Pack(prefix_extractor_name),
                     prefix_extractor_name.length()));
  string_package_.push_back(
      std::make_pair(shm_package::Pack(property_collectors_names),
                     property_collectors_names.length()));
  string_package_.push_back(std::make_pair(shm_package::Pack(compression_name),
                                           compression_name.length()));
  string_package_.push_back(std::make_pair(
      shm_package::Pack(compression_options), compression_options.length()));
  string_package_.push_back(
      std::make_pair(shm_package::Pack(seqno_to_time_mapping),
                     seqno_to_time_mapping.length()));

  //    user_collected_properties_package_
  for (auto& iter : user_collected_properties) {
    user_collected_properties_package_.push_back(
        std::make_pair(shm_package::Pack(iter.first), iter.first.length()));
    user_collected_properties_package_.push_back(
        std::make_pair(shm_package::Pack(iter.second), iter.second.length()));
  }
  //   readable_properties_package_
  for (auto& iter : readable_properties) {
    readable_properties_package_.push_back(
        std::make_pair(shm_package::Pack(iter.first), iter.first.length()));
    readable_properties_package_.push_back(
        std::make_pair(shm_package::Pack(iter.second), iter.second.length()));
  }
  is_packaged_ = true;
}
void TableProperties::UnPack() {
  if (!is_packaged_) {
    LOG("TableProperties has not been packed, can't unpack.");
    return;
  }
  shm_package::Unpack(string_package_[0].first, db_id,
                      string_package_[0].second);
  shm_package::Unpack(string_package_[1].first, db_session_id,
                      string_package_[1].second);
  shm_package::Unpack(string_package_[2].first, db_host_id,
                      string_package_[2].second);
  shm_package::Unpack(string_package_[3].first, column_family_name,
                      string_package_[3].second);
  shm_package::Unpack(string_package_[4].first, filter_policy_name,
                      string_package_[4].second);
  shm_package::Unpack(string_package_[5].first, comparator_name,
                      string_package_[5].second);
  shm_package::Unpack(string_package_[6].first, merge_operator_name,
                      string_package_[6].second);
  shm_package::Unpack(string_package_[7].first, prefix_extractor_name,
                      string_package_[7].second);
  shm_package::Unpack(string_package_[8].first, property_collectors_names,
                      string_package_[8].second);
  shm_package::Unpack(string_package_[9].first, compression_name,
                      string_package_[9].second);
  shm_package::Unpack(string_package_[10].first, compression_options,
                      string_package_[10].second);
  shm_package::Unpack(string_package_[11].first, seqno_to_time_mapping,
                      string_package_[11].second);
  user_collected_properties.clear();  // assert
  readable_properties.clear();        // assert
  for (size_t i = 0; i < user_collected_properties_package_.size(); i += 2) {
    std::string first, second;
    shm_package::Unpack(user_collected_properties_package_[i].first, first,
                        user_collected_properties_package_[i].second);
    shm_package::Unpack(user_collected_properties_package_[i + 1].first, second,
                        user_collected_properties_package_[i + 1].second);
    user_collected_properties.insert(std::make_pair(first, second));
  }
  for (size_t i = 0; i < readable_properties_package_.size(); i += 2) {
    std::string first, second;
    shm_package::Unpack(readable_properties_package_[i].first, first,
                        readable_properties_package_[i].second);
    shm_package::Unpack(readable_properties_package_[i + 1].first, second,
                        readable_properties_package_[i + 1].second);
    readable_properties.insert(std::make_pair(first, second));
  }
  string_package_.clear();
  user_collected_properties_package_.clear();
  readable_properties_package_.clear();
  is_packaged_ = false;
}

}  // namespace ROCKSDB_NAMESPACE
