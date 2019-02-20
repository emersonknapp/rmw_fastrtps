// Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef RMW_FASTRTPS_SHARED_CPP__TOPIC_CACHE_HPP_
#define RMW_FASTRTPS_SHARED_CPP__TOPIC_CACHE_HPP_

#include <algorithm>
#include <iterator>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "fastrtps/participant/Participant.h"
#include "fastrtps/rtps/common/Guid.h"
#include "fastrtps/rtps/common/InstanceHandle.h"
#include "rcutils/logging_macros.h"

#include "rmw_fastrtps_shared_cpp/thread_safety_annotations.hpp"

typedef eprosima::fastrtps::rtps::GUID_t GUID_t;

/**
 * Topic cache data structure. Manages relationships between participants and topics.
 */
typedef std::unordered_map<std::string, std::vector<std::string>> NameToNamesMap;

class TopicCache
{
private:
  typedef std::map<GUID_t, NameToNamesMap> ParticipantTopicMap;
  typedef NameToNamesMap TopicToTypes;

  /**
   * Guards TopicToTypes and ParticipantTopicMap - for atomic access to each
   * individually, as well as to keep their topic sets in sync
   */
  mutable std::mutex mutex_;

  /**
   * Map of topic names to a vector of types that topic may use.
   * Topics here are represented as one to many, DDS XTypes 1.2
   * specifies application code 'generally' uses a 1-1 relationship.
   * However, generic services such as logger and monitor, can discover
   * multiple types on the same topic.
   *
   */
  TopicToTypes topic_to_types_ R2_GUARDED_BY(mutex_);

  /**
   * Map of participant GUIDS to a set of topic-type.
   */
  ParticipantTopicMap participant_to_topics_ R2_GUARDED_BY(mutex_);

  /**
   * Helper function to initialize a topic vector.
   *
   * @param topic_name
   */
  void initializeTopic(const std::string & topic_name, TopicToTypes & topic_to_types)
  {
    if (topic_to_types.find(topic_name) == topic_to_types.end()) {
      topic_to_types[topic_name] = std::vector<std::string>();
    }
  }

  /**
   * Helper function to initialize the set inside a participant map.
   *
   * @param map
   * @param guid
   */
  void initializeParticipantMap(
    ParticipantTopicMap & map,
    GUID_t guid)
  {
    if (map.find(guid) == map.end()) {
      map[guid] = TopicToTypes();
    }
  }

public:
  /**
   * @param participant_guid
   * @param topics On successful find, topic name map will be copied to this location
   * @return whether guid was found in participant list
   */
  bool cloneParticipantTopics(const GUID_t & participant_guid, NameToNamesMap * topics) const
  R2_REQUIRES(!mutex_)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = participant_to_topics_.find(participant_guid);
    if (it == participant_to_topics_.end()) {
      return false;
    }
    *topics = it->second;
    return true;
  }

  /**
   * @return a copy of a map of topic name to the vector of topic types used.
   */
  NameToNamesMap cloneTopicToTypes() const R2_REQUIRES(!mutex_)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return topic_to_types_;
  }

  /**
   * Add a topic based on discovery.
   *
   * @param rtpsParticipantKey
   * @param topic_name
   * @param type_name
   * @return true if a change has been recorded
   */
  bool addTopic(
    const eprosima::fastrtps::rtps::InstanceHandle_t & rtpsParticipantKey,
    const std::string & topic_name,
    const std::string & type_name) R2_REQUIRES(!mutex_)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    initializeTopic(topic_name, topic_to_types_);
    auto guid = iHandle2GUID(rtpsParticipantKey);
    initializeParticipantMap(participant_to_topics_, guid);
    initializeTopic(topic_name, participant_to_topics_[guid]);
    if (rcutils_logging_logger_is_enabled_for("rmw_fastrtps_shared_cpp",
      RCUTILS_LOG_SEVERITY_DEBUG))
    {
      std::stringstream guid_stream;
      guid_stream << guid;
      RCUTILS_LOG_DEBUG_NAMED(
        "rmw_fastrtps_shared_cpp",
        "Adding topic '%s' with type '%s' for node '%s'",
        topic_name.c_str(), type_name.c_str(), guid_stream.str().c_str());
    }
    topic_to_types_[topic_name].push_back(type_name);
    participant_to_topics_[guid][topic_name].push_back(type_name);
    return true;
  }

  /**
   * Remove a topic based on discovery.
   *
   * @param rtpsParticipantKey
   * @param topic_name
   * @param type_name
   * @return true if a change has been recorded
   */
  bool removeTopic(
    const eprosima::fastrtps::rtps::InstanceHandle_t & rtpsParticipantKey,
    const std::string & topic_name,
    const std::string & type_name) R2_REQUIRES(!mutex_)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (topic_to_types_.find(topic_name) == topic_to_types_.end()) {
      RCUTILS_LOG_DEBUG_NAMED(
        "rmw_fastrtps_shared_cpp",
        "unexpected removal on topic '%s' with type '%s'",
        topic_name.c_str(), type_name.c_str());
      return false;
    }
    {
      auto & type_vec = topic_to_types_[topic_name];
      type_vec.erase(std::find(type_vec.begin(), type_vec.end(), type_name));
      if (type_vec.empty()) {
        topic_to_types_.erase(topic_name);
      }
    }

    auto guid = iHandle2GUID(rtpsParticipantKey);
    auto guid_topics_pair = participant_to_topics_.find(guid);
    if (guid_topics_pair != participant_to_topics_.end() &&
      guid_topics_pair->second.find(topic_name) != guid_topics_pair->second.end())
    {
      auto & type_vec = guid_topics_pair->second[topic_name];
      type_vec.erase(std::find(type_vec.begin(), type_vec.end(), type_name));
      if (type_vec.empty()) {
        participant_to_topics_[guid].erase(topic_name);
      }
      if (participant_to_topics_[guid].empty()) {
        participant_to_topics_.erase(guid);
      }
    } else {
      RCUTILS_LOG_DEBUG_NAMED(
        "rmw_fastrtps_shared_cpp",
        "Unable to remove topic, does not exist '%s' with type '%s'",
        topic_name.c_str(), type_name.c_str());
    }
    return true;
  }

  size_t countParticipants(const std::vector<std::string> & fqdns) R2_REQUIRES(!mutex_)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t count = 0;
    // Search and sum up the participant counts
    for (const auto & topic_fqdn : fqdns) {
      const auto & it = topic_to_types_.find(topic_fqdn);
      if (it != topic_to_types_.end()) {
        count += it->second.size();
      }
    }
    return count;
  }


  friend std::ostream & operator<<(std::ostream & os, const TopicCache & topic_cache);
};

inline std::ostream & operator<<(
  std::ostream & ostream,
  const TopicCache & topic_cache)
{
  std::lock_guard<std::mutex> lock(topic_cache.mutex_);
  std::stringstream map_ss;
  map_ss << "Participant Info: " << std::endl;
  for (auto & elem : topic_cache.participant_to_topics_) {
    std::ostringstream stream;
    stream << "  Topics: " << std::endl;
    for (auto & types : elem.second) {
      stream << "    " << types.first << ": ";
      std::copy(types.second.begin(), types.second.end(),
        std::ostream_iterator<std::string>(stream, ","));
      stream << std::endl;
    }
    map_ss << elem.first << std::endl << stream.str();
  }
  std::stringstream topics_ss;
  topics_ss << "Cumulative TopicToTypes: " << std::endl;
  for (auto & elem : topic_cache.topic_to_types_) {
    std::ostringstream stream;
    std::copy(elem.second.begin(), elem.second.end(), std::ostream_iterator<std::string>(stream,
      ","));
    topics_ss << "  " << elem.first << " : " << stream.str() << std::endl;
  }
  ostream << map_ss.str() << topics_ss.str();
  return ostream;
}

#endif  // RMW_FASTRTPS_SHARED_CPP__TOPIC_CACHE_HPP_
