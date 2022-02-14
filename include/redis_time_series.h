#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <exception>
#include <fmt/format.h>
#include <iomanip>
#include <optional>
#include <sstream>

#include <sw/redis++/pipeline.h>
#include <sw/redis++/redis++.h>

namespace redis_time_series {

namespace command {
constexpr char CREATE[] = "TS.CREATE";
constexpr char ALTER[] = "TS.ALTER";
constexpr char ADD[] = "TS.ADD";
constexpr char MADD[] = "TS.MADD";
constexpr char INCRBY[] = "TS.INCRBY";
constexpr char DECRBY[] = "TS.DECRBY";
constexpr char DEL[] = "TS.DEL";
constexpr char CREATERULE[] = "TS.CREATERULE";
constexpr char DELETERULE[] = "TS.DELETERULE";
constexpr char RANGE[] = "TS.RANGE";
constexpr char REVRANGE[] = "TS.REVRANGE";
constexpr char MRANGE[] = "TS.MRANGE";
constexpr char MREVRANGE[] = "TS.MREVRANGE";
constexpr char GET[] = "TS.GET";
constexpr char MGET[] = "TS.MGET";
constexpr char INFO[] = "TS.INFO";
constexpr char QUERYINDEX[] = "TS.QUERYINDEX";
} // namespace command

namespace command_args {
constexpr char RETENTION[] = "RETENTION";
constexpr char LABELS[] = "LABELS";
constexpr char UNCOMPRESSED[] = "UNCOMPRESSED";
constexpr char COUNT[] = "COUNT";
constexpr char AGGREGATION[] = "AGGREGATION";
constexpr char ALIGN[] = "ALIGN";
constexpr char FILTER[] = "FILTER";
constexpr char WITHLABELS[] = "WITHLABELS";
constexpr char SELECTEDLABELS[] = "SELECTED_LABELS";
constexpr char TIMESTAMP[] = "TIMESTAMP";
constexpr char CHUNK_SIZE[] = "CHUNK_SIZE";
constexpr char DUPLICATE_POLICY[] = "DUPLICATE_POLICY";
constexpr char ON_DUPLICATE[] = "ON_DUPLICATE";
constexpr char GROPUBY[] = "GROUPBY";
constexpr char REDUCE[] = "REDUCE";
constexpr char FILTER_BY_TS[] = "FILTER_BY_TS";
constexpr char FILTER_BY_VALUE[] = "FILTER_BY_VALUE";
} // namespace command_args

namespace command_operator {
enum class TsAggregation {
    AVG,
    SUM,
    MIN,
    MAX,
    RANGE,
    COUNT,
    FIRST,
    LAST,
    STDP,
    STDS,
    VARP,
    VARS
};

inline std::string to_string(TsAggregation aggrgation) {
    switch (aggrgation) {
    case TsAggregation::AVG:
        return "AVG";
    case TsAggregation::SUM:
        return "SUM";
    case TsAggregation::MAX:
        return "MAX";
    case TsAggregation::COUNT:
        return "COUNT";
    case TsAggregation::FIRST:
        return "FIRST";
    case TsAggregation::STDP:
        return "STD.P";
    case TsAggregation::VARP:
        return "VAR.P";
    case TsAggregation::VARS:
        return "VAR.S";
    default:
        throw std::out_of_range("Invalid aggregation type.");
    }
}

inline TsAggregation to_aggregation(const std::string &aggregation) {
    if (aggregation == "AVG") return TsAggregation::AVG;
    if (aggregation == "SUM") return TsAggregation::SUM;
    if (aggregation == "MIN") return TsAggregation::MIN;
    if (aggregation == "MAX") return TsAggregation::MAX;
    if (aggregation == "RANGE") return TsAggregation::RANGE;
    if (aggregation == "COUNT") return TsAggregation::COUNT;
    if (aggregation == "FIRST") return TsAggregation::FIRST;
    if (aggregation == "LAST") return TsAggregation::LAST;
    if (aggregation == "STD.P") return TsAggregation::STDP;
    if (aggregation == "STD.S") return TsAggregation::STDS;
    if (aggregation == "VAR.P") return TsAggregation::VARP;
    if (aggregation == "VAR.S") return TsAggregation::VARS;
    throw std::out_of_range("Invalid aggregation type '{aggregation}'");
}

enum class TsDuplicatePolicy { BLOCK, FIRST, LAST, MIN, MAX, SUM };
inline std::string to_string(TsDuplicatePolicy policy) {
    switch (policy) {
    case TsDuplicatePolicy::BLOCK:
        return "BLOCK";
    case TsDuplicatePolicy::FIRST:
        return "FIRST";
    case TsDuplicatePolicy::LAST:
        return "LAST";
    case TsDuplicatePolicy::MIN:
        return "MIN";
    case TsDuplicatePolicy::MAX:
        return "MAX";
    case TsDuplicatePolicy::SUM:
        return "SUM";
    default:
        throw std::out_of_range("Invalid policy type.");
    }
}

inline TsDuplicatePolicy to_duplicatPolicy(const std::string &policy) {
    if (policy == "BLOCK") return TsDuplicatePolicy::BLOCK;
    if (policy == "FIRST") return TsDuplicatePolicy::FIRST;
    if (policy == "LAST") return TsDuplicatePolicy::LAST;
    if (policy == "MIN") return TsDuplicatePolicy::MIN;
    if (policy == "MAX") return TsDuplicatePolicy::MAX;
    if (policy == "SUM") return TsDuplicatePolicy::SUM;
    throw std::out_of_range("Invalid policy type '{policy}'");
}

enum class TsReduce { SUM, MIN, MAX };
inline std::string to_string(TsReduce reduce) {
    switch (reduce) {
    case TsReduce::SUM:
        return "SUM";
    case TsReduce::MIN:
        return "MIN";
    case TsReduce::MAX:
        return "MAX";
    default:
        throw std::out_of_range("Invalid reduce type.");
    }
}
} // namespace command_operator

class TimeStamp {
  public:
    const std::array<std::string_view, 3> constants{"-", "+", "*"};
    
    TimeStamp(uint64_t timestamp) : value_{timestamp} {}
    TimeStamp(const std::string& timestamp) {
        auto res = std::find_if(
            constants.begin(), constants.end(),
            [&timestamp](const auto &str) { return str == timestamp; });
        if (res == constants.end()) throw std::invalid_argument("Timestamp parameter is wrong");

        constantTime_ = timestamp;
        isConstant_ = true;
    }

    TimeStamp(const std::chrono::system_clock::time_point &dateTime) {
        value_ = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                dateTime.time_since_epoch())
                .count());
    }

    TimeStamp(const std::string &timestamp, const std::string &format) {
        std::tm tm = {};
        std::stringstream ss(timestamp);
        ss >> std::get_time(&tm, format.data());
        auto tp = std::chrono::system_clock::from_time_t(std::mktime(&tm));
        value_ = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                tp.time_since_epoch())
                .count());
    }

    TimeStamp() = default;
    ~TimeStamp() = default;
    TimeStamp(const TimeStamp &timestamp) : value_{timestamp.value_} {}
    TimeStamp(TimeStamp &&timestamp) : value_{timestamp.value_} {}
    TimeStamp &operator=(const TimeStamp &timestamp) {
        value_ = timestamp.value_;
        isConstant_ = timestamp.isConstant_;
        constantTime_ = timestamp.constantTime_;
        return *this;
    }
    TimeStamp &operator=(TimeStamp &&timestamp) {
        value_ = timestamp.value_;
        isConstant_ = timestamp.isConstant_;
        constantTime_ = timestamp.constantTime_;
        return *this;
    }

    bool hasValue() const { return value_ > 0 || !constantTime_.empty(); }
    //TODO:
    uint64_t value() const { return value_; } 

    std::string to_string() const {
        if (isConstant_) return constantTime_;
        return fmt::format("{}", value_);
    }

    friend bool operator==(const TimeStamp &lhs, const TimeStamp &rhs) {
        return lhs.value_ == rhs.value_;
    }

    friend bool operator<(const TimeStamp &lhs, const TimeStamp &rhs) {
        return lhs.value_ < rhs.value_;
    }

  private:
    bool isConstant_{false};
    std::string constantTime_{};
    uint64_t value_{};
};

class TimeSeriesTuple {
  public:
    TimeSeriesTuple() = default;
    TimeSeriesTuple(const TimeStamp &time, double val)
        : time_{time}, value_{val} {}

    TimeStamp time() const { return time_; }
    double value() const { return value_; }

  private:
    TimeStamp time_;
    double value_{};
};

class TimeSeriesRule {
  public:
    TimeSeriesRule(const std::string &destKey, uint64_t timeBucket,
                   std::optional<command_operator::TsAggregation> aggregation)
        : destKey_{destKey_}, timeBucket_{timeBucket}, aggregation_{
                                                           aggregation} {}
    std::string destKey() const { return destKey_; }
    uint64_t timeBucket() const { return timeBucket_; }
    std::optional<command_operator::TsAggregation> aggregation() const {
        return aggregation_;
    }

  private:
    std::string destKey_;
    uint64_t timeBucket_{};
    std::optional<command_operator::TsAggregation> aggregation_;
};

class TimeSeriesLabel {
  public:
    TimeSeriesLabel(const std::string &key, const std::string &value)
        : key_{key}, value_{value} {}
    std::string key() const { return key_; }
    std::string value() const { return value_; }

    friend bool operator==(const TimeSeriesLabel &lhs,
                           const TimeSeriesLabel &rhs) {
        return (lhs.value_ == rhs.value_ && lhs.key_ == rhs.key_);
    }

  private:
    std::string key_;
    std::string value_;
};

class TimeSeriesInformation {
  public:
    TimeSeriesInformation(
        uint64_t totalSamples, uint64_t memoryUsage,
        const TimeStamp &firstTimeStamp, const TimeStamp &lastTimeStamp,
        uint64_t retentionTime, uint64_t chunkCount, uint64_t chunkSize,
        const std::vector<TimeSeriesLabel> &labels,
        const std::string &sourceKey, const std::vector<TimeSeriesRule> &rules,
        std::optional<command_operator::TsDuplicatePolicy> policy)
        : totalSamples_{totalSamples}, memoryUsage_{memoryUsage},
          firstTimeStamp_{firstTimeStamp}, lastTimeStamp_{lastTimeStamp},
          retentionTime_{retentionTime}, chunkCount_{chunkCount},
          chunkSize_{chunkSize}, labels_{labels},
          sourceKey_{sourceKey}, rules_{rules}, duplicatePolicy_{policy} {}

    uint64_t totalSamples() const { return totalSamples_; }
    uint64_t memoryUsage() const { return memoryUsage_; }
    TimeStamp firstTimeStamp() const { return firstTimeStamp_; }
    TimeStamp lastTimeStamp() const { return lastTimeStamp_; }
    uint64_t retentionTime() const { return retentionTime_; }
    uint64_t chunkCount() const { return chunkCount_; }
    uint64_t chunkSize() const { return chunkSize_; }
    std::vector<TimeSeriesLabel> labels() const { return labels_; }
    std::string sourceKey() const { return sourceKey_; }
    std::vector<TimeSeriesRule> rules() const { return rules_; }
    std::optional<command_operator::TsDuplicatePolicy> duplicatePolicy() const {
        return duplicatePolicy_;
    }

  private:
    uint64_t totalSamples_{};
    uint64_t memoryUsage_{};
    TimeStamp firstTimeStamp_{};
    TimeStamp lastTimeStamp_{};
    uint64_t retentionTime_{};
    uint64_t chunkCount_{};
    uint64_t chunkSize_{};
    std::vector<TimeSeriesLabel> labels_;
    std::string sourceKey_;
    std::vector<TimeSeriesRule> rules_;
    std::optional<command_operator::TsDuplicatePolicy> duplicatePolicy_;
};

namespace aux {
inline void addRetentionTime(std::vector<std::string> &args,
                             std::optional<uint64_t> retentionTime) {
    if (retentionTime.has_value()) {
        args.push_back(command_args::RETENTION);
        args.push_back(std::to_string(retentionTime.value()));
    }
}

inline void addChunkSize(std::vector<std::string> &args,
                         std::optional<uint64_t> chunkSize) {
    if (chunkSize.has_value()) {
        args.push_back(command_args::CHUNK_SIZE);
        args.push_back(std::to_string(chunkSize.value()));
    }
}

inline void addLabels(std::vector<std::string> &args,
                      std::vector<TimeSeriesLabel> labels) {
    if (labels.size() > 0) {
        args.push_back(command_args::LABELS);
        for (auto &label : labels) {
            args.push_back(label.key());
            args.push_back(label.value());
        }
    }
}

inline void addUncompressed(std::vector<std::string> &args,
                            std::optional<bool> uncompressed) {
    if (uncompressed.has_value()) {
        args.push_back(command_args::UNCOMPRESSED);
    }
}

inline void addCount(std::vector<std::string> &args,
                     std::optional<uint64_t> count) {
    if (count.has_value()) {
        args.push_back(command_args::COUNT);
        args.push_back(std::to_string(count.value()));
    }
}

inline void
addDuplicatePolicy(std::vector<std::string> &args,
                   std::optional<command_operator::TsDuplicatePolicy> policy) {
    if (policy.has_value()) {
        args.push_back(command_args::DUPLICATE_POLICY);
        args.push_back(command_operator::to_string(policy.value()));
    }
}

inline void
addOnDuplicate(std::vector<std::string> &args,
               std::optional<command_operator::TsDuplicatePolicy> policy) {
    if (policy.has_value()) {
        args.push_back(command_args::ON_DUPLICATE);
        args.push_back(command_operator::to_string(policy.value()));
    }
}

inline void addAlign(std::vector<std::string> &args, const TimeStamp &align) {
    if (!align.hasValue()) {
        args.push_back(command_args::ALIGN);
        args.push_back(align.to_string());
    }
}

inline void
addAggregation(std::vector<std::string> &args,
               std::optional<command_operator::TsAggregation> aggregation,
               std::optional<uint64_t> timeBucket) {
    if (aggregation.has_value()) {
        args.push_back(command_args::AGGREGATION);
        args.push_back(command_operator::to_string(aggregation.value()));
        if (!timeBucket.has_value()) {
            throw std::invalid_argument(
                "RANGE Aggregation should have timeBucket value");
        }
        args.push_back(std::to_string(timeBucket.value()));
    }
}

inline void addFilters(std::vector<std::string> &args,
                       const std::vector<std::string> &filter) {
    if (filter.size() == 0) {
        throw std::invalid_argument(
            "There should be at least one filter on MRANGE/MREVRANGE");
    }
    args.push_back(command_args::FILTER);
    for (auto &f : filter) {
        args.push_back(f);
    }
}

inline void addFilterByTs(std::vector<std::string> &args,
                          const std::vector<TimeStamp> &filter) {
    if (filter.size() != 0) {
        args.push_back(command_args::FILTER_BY_TS);
        for (auto &ts : filter) {
            args.push_back(ts.to_string());
        }
    }
}

inline void
addFilterByValue(std::vector<std::string> &args,
                 std::optional<std::pair<uint64_t, uint64_t>> filter) {
    if (filter.has_value()) {
        args.push_back(command_args::FILTER_BY_VALUE);
        args.push_back(std::to_string(filter.value().first));
        args.push_back(std::to_string(filter.value().second));
    }
}

inline void addWithLabels(std::vector<std::string> &args,
                          std::optional<bool> withLabels,
                          const std::vector<std::string> &selectLabels = {}) {
    if (withLabels.has_value() && selectLabels.size() != 0) {
        throw std::invalid_argument(
            "withLabels and selectLabels cannot be specified together.");
    }

    if (withLabels.has_value() && withLabels.has_value() &&
        withLabels.value()) {
        args.push_back(command_args::WITHLABELS);
    }

    if (selectLabels.size() != 0) {
        args.push_back(command_args::SELECTEDLABELS);
        for (auto &label : selectLabels) {
            args.push_back(label);
        }
    }
}

inline void addGroupby(std::vector<std::string> &args,
                       std::optional<std::string> groupby,
                       std::optional<command_operator::TsReduce> reduce) {
    if (groupby.has_value() && reduce.has_value()) {
        args.push_back(command_args::GROPUBY);
        args.push_back(groupby.value());
        args.push_back(command_args::REDUCE);
        args.push_back(command_operator::to_string(reduce.value()));
    }
}

inline void addTimeStamp(std::vector<std::string> &args,
                         const TimeStamp &timeStamp) {
    if (!timeStamp.hasValue()) {
        args.push_back(command_args::TIMESTAMP);
        args.push_back(timeStamp.to_string());
    }
}

inline void addRule(std::vector<std::string> &args,
                    const TimeSeriesRule &rule) {
    args.push_back(rule.destKey());
    args.push_back(command_args::AGGREGATION);
    args.push_back(command_operator::to_string(rule.aggregation().value()));
    args.push_back(std::to_string(rule.timeBucket()));
}

inline std::vector<std::string>
buildTsCreateArgs(const std::string &key, std::optional<uint64_t> retentionTime,
                  const std::vector<TimeSeriesLabel> &labels,
                  std::optional<bool> uncompressed,
                  std::optional<uint64_t> chunkSizeBytes,
                  std::optional<command_operator::TsDuplicatePolicy> policy) {
    std::vector<std::string> args{key};
    addRetentionTime(args, retentionTime);
    addChunkSize(args, chunkSizeBytes);
    addLabels(args, labels);
    addUncompressed(args, uncompressed);
    addDuplicatePolicy(args, policy);
    return args;
}

inline std::vector<std::string>
buildTsAlterArgs(const std::string &key, std::optional<uint64_t> retentionTime,
                 const std::vector<TimeSeriesLabel> &labels) {
    std::vector<std::string> args{key};
    addRetentionTime(args, retentionTime);
    addLabels(args, labels);
    return args;
}

inline std::vector<std::string>
buildTsAddArgs(const std::string &key, const TimeStamp &timestamp, double value,
               std::optional<uint64_t> retentionTime,
               const std::vector<TimeSeriesLabel> &labels,
               std::optional<bool> uncompressed,
               std::optional<uint64_t> chunkSizeBytes,
               std::optional<command_operator::TsDuplicatePolicy> policy) {
    std::vector<std::string> args{key, timestamp.to_string(),
                                  std::to_string(value)};
    addRetentionTime(args, retentionTime);
    addChunkSize(args, chunkSizeBytes);
    addLabels(args, labels);
    addUncompressed(args, uncompressed);
    addOnDuplicate(args, policy);
    return args;
}

inline std::vector<std::string> buildTsIncrDecrByArgs(
    const std::string &key, double value, const TimeStamp &timestamp,
    std::optional<uint64_t> retentionTime,
    const std::vector<TimeSeriesLabel> &labels,
    std::optional<bool> uncompressed, std::optional<uint64_t> chunkSizeBytes) {
    std::vector<std::string> args{key, std::to_string(value)};
    addTimeStamp(args, timestamp);
    addRetentionTime(args, retentionTime);
    addChunkSize(args, chunkSizeBytes);
    addLabels(args, labels);
    addUncompressed(args, uncompressed);
    return args;
}

inline std::vector<std::string> buildTsDelArgs(const std::string &key,
                                               const TimeStamp &fromTimeStamp,
                                               const TimeStamp &toTimeStamp) {
    std::vector<std::string> args{key, fromTimeStamp.to_string(),
                                  toTimeStamp.to_string()};
    return args;
}

inline std::vector<std::string> buildTsMaddArgs(
    const std::vector<std::tuple<std::string, TimeStamp, double>> &sequence) {
    std::vector<std::string> args;
    for (auto &tuple : sequence) {
        args.push_back(std::get<0>(tuple));
        args.push_back(std::get<1>(tuple).to_string());
        args.push_back(std::to_string(std::get<2>(tuple)));
    }
    return args;
}

inline std::vector<std::string>
buildTsMgetArgs(const std::vector<std::string> &filter,
                std::optional<bool> withLabels) {
    std::vector<std::string> args;
    addWithLabels(args, withLabels);
    addFilters(args, filter);
    return args;
}

inline std::vector<std::string>
buildRangeArgs(const std::string &key, const TimeStamp &fromTimeStamp,
               const TimeStamp &toTimeStamp, std::optional<uint64_t> count,
               std::optional<command_operator::TsAggregation> aggregation,
               std::optional<uint64_t> timeBucket,
               const std::vector<TimeStamp> &filterByTs,
               std::optional<std::pair<uint64_t, uint64_t>> filterByValue,
               const TimeStamp &align) {
    std::vector<std::string> args{key, fromTimeStamp.to_string(),
                                  toTimeStamp.to_string()};
    addFilterByTs(args, filterByTs);
    addFilterByValue(args, filterByValue);
    addCount(args, count);
    addAlign(args, align);
    addAggregation(args, aggregation, timeBucket);
    return args;
}

inline std::vector<std::string> buildMultiRangeArgs(
    const TimeStamp &fromTimeStamp, const TimeStamp &toTimeStamp,
    const std::vector<std::string> &filter, std::optional<uint64_t> count,
    std::optional<command_operator::TsAggregation> aggregation,
    std::optional<uint64_t> timeBucket, std::optional<bool> withLabels,
    std::optional<std::string> groupby,
    std::optional<command_operator::TsReduce> reduse,
    const std::vector<TimeStamp> &filterByTs,
    std::optional<std::pair<uint64_t, uint64_t>> filterByValue,
    const std::vector<std::string> &selectLabels, const TimeStamp &align) {

    std::vector<std::string> args{fromTimeStamp.to_string(),
                                  toTimeStamp.to_string()};
    addFilterByTs(args, filterByTs);
    addFilterByValue(args, filterByValue);
    addCount(args, count);
    addAlign(args, align);
    addAggregation(args, aggregation, timeBucket);
    addWithLabels(args, withLabels, selectLabels);
    addFilters(args, filter);
    addGroupby(args, groupby, reduse);
    return args;
}
}; // namespace aux

namespace parser {
inline bool parseBoolean(const sw::redis::OptionalString &result) {
    return result && *result == "OK";
}

inline long parseLong(uint64_t result) { return result; }

inline TimeStamp parseTimeStamp(uint64_t result) { return TimeStamp(result); }

inline std::vector<TimeStamp>
parseTimeStampArray(std::vector<long long> result) {
    std::vector<TimeStamp> list;
    for (auto &res : result)
        list.push_back(TimeStamp(static_cast<uint64_t>(res)));
    return list;
}

inline TimeSeriesTuple
parseTimeSeriesTuple(const std::tuple<std::string, std::string> &result) {
    return TimeSeriesTuple(TimeStamp(std::get<0>(result), ""),
                           std::stod(std::get<1>(result)));
}

inline std::vector<TimeSeriesTuple> parseTimeSeriesTupleArray(
    const std::vector<std::tuple<std::string, std::string>> &result) {
    std::vector<TimeSeriesTuple> list;
    for (auto &res : result)
        list.push_back(parseTimeSeriesTuple(res));
    return list;
}

inline std::vector<TimeSeriesLabel> parseLabelArray(
    const std::vector<std::tuple<std::string, std::string>> &result) {
    std::vector<TimeSeriesLabel> list;
    for (auto &res : result) {
        list.push_back(TimeSeriesLabel(std::get<0>(res), std::get<1>(res)));
    }
    return list;
}

// private
// static std::vector<(string key, std::vector<TimeSeriesLabel> labels,
//                       TimeSeriesTuple value)>
// ParseMGetesponse(RedisResult result) {
//     RedisResult[] redisResults = (RedisResult[])result;
//     var list = new List<(string key, std::vector<TimeSeriesLabel> labels,
//                          TimeSeriesTuple values)>(redisResults.Length);
//     if (redisResults.Length == 0) return list;
//     Array.ForEach(
//         redisResults, MRangeValue = > {
//             RedisResult[] MRangeTuple = (RedisResult[])MRangeValue;
//            const std::string&key = (string)MRangeTuple[0];
//             std::vector<TimeSeriesLabel> labels =
//                 ParseLabelArray(MRangeTuple[1]);
//             TimeSeriesTuple value = ParseTimeSeriesTuple(MRangeTuple[2]);
//             list.Add((key, labels, value));
//         });
//     return list;
// }

// private
// static std::vector<(string key, std::vector<TimeSeriesLabel> labels,
//                       std::vector<TimeSeriesTuple> values)>
// ParseMRangeResponse(RedisResult result) {
//     RedisResult[] redisResults = (RedisResult[])result;
//     var list =
//         new List<(string key, std::vector<TimeSeriesLabel> labels,
//                   std::vector<TimeSeriesTuple>
//                   values)>(redisResults.Length);
//     if (redisResults.Length == 0) return list;
//     Array.ForEach(
//         redisResults, MRangeValue = > {
//             RedisResult[] MRangeTuple = (RedisResult[])MRangeValue;
//            const std::string&key = (string)MRangeTuple[0];
//             std::vector<TimeSeriesLabel> labels =
//                 ParseLabelArray(MRangeTuple[1]);
//             std::vector<TimeSeriesTuple> values =
//                 ParseTimeSeriesTupleArray(MRangeTuple[2]);
//             list.Add((key, labels, values));
//         });
//     return list;
// }

inline TimeSeriesRule
parseRule(const std::tuple<std::string, std::string, sw::redis::OptionalString>
              &result) {
    auto destKey = std::get<0>(result);
    uint64_t bucketTime = std::atoll(std::get<1>(result).c_str());
    auto agg = std::get<2>(result);
    std::optional<command_operator::TsAggregation> aggregation = std::nullopt;
    if (agg.has_value())
        aggregation = command_operator::to_aggregation(agg.value());
    return TimeSeriesRule{destKey, bucketTime, aggregation};
}

inline std::vector<TimeSeriesRule> parseRuleArray(
    const std::vector<std::tuple<std::string, std::string,
                                 sw::redis::OptionalString>> &result) {
    std::vector<TimeSeriesRule> list;
    for (auto &res : result) {
        list.push_back(parseRule(res));
    }
    return list;
}

inline std::optional<command_operator::TsDuplicatePolicy>
parsePolicy(const sw::redis::OptionalString &result) {
    if (result.has_value())
        return command_operator::to_duplicatPolicy(result.value());
    return std::nullopt;
}

inline TimeSeriesInformation parseInfo(redisReply *reply) {
    if (!sw::redis::reply::is_array(*reply)) {
        throw sw::redis::ProtoError("Expect ARRAY reply");
    }

    uint64_t totalSamples = 0, memoryUsage = 0, retentionTime = 0,
             chunkSize = 0, chunkCount = 0;
    TimeStamp firstTimestamp, lastTimestamp;
    std::vector<TimeSeriesLabel> labels;
    std::vector<TimeSeriesRule> rules;
    std::string sourceKey;
    std::optional<command_operator::TsDuplicatePolicy> duplicatePolicy;

    for (size_t i = 0; i < reply->elements; ++i) {
        auto key = sw::redis::reply::parse<std::string>(*reply->element[i++]);
        if (key == "totalSamples") {
            totalSamples =
                sw::redis::reply::parse<long long>(*reply->element[i]);
        } else if (key == "memoryUsage") {
            memoryUsage =
                sw::redis::reply::parse<long long>(*reply->element[i]);
        } else if (key == "retentionTime") {
            retentionTime =
                sw::redis::reply::parse<long long>(*reply->element[i]);
        } else if (key == "chunkCount") {
            chunkCount = sw::redis::reply::parse<long long>(*reply->element[i]);
        } else if (key == "chunkSize") {
            chunkSize = sw::redis::reply::parse<long long>(*reply->element[i]);
        } else if (key == "firstTimestamp") {
            firstTimestamp = parseTimeStamp(
                sw::redis::reply::parse<long long>(*reply->element[i]));
        } else if (key == "lastTimestamp") {
            lastTimestamp = parseTimeStamp(
                sw::redis::reply::parse<long long>(*reply->element[i]));
        } else if (key == "labels") {
            labels = parseLabelArray(
                sw::redis::reply::parse<
                    std::vector<std::tuple<std::string, std::string>>>(
                    *reply->element[i]));
        } else if (key == "sourceKey") {
            auto src = sw::redis::reply::parse<sw::redis::OptionalString>(
                *reply->element[i]);
            if (src.has_value()) sourceKey = src.value();
        } else if (key == "rules") {
            rules = parseRuleArray(
                sw::redis::reply::parse<std::vector<std::tuple<
                    std::string, std::string, sw::redis::OptionalString>>>(
                    *reply->element[i]));
        } else if (key == "duplicatePolicy") {
            duplicatePolicy =
                parsePolicy(sw::redis::reply::parse<sw::redis::OptionalString>(
                    *reply->element[i]));
        }
    }
    return TimeSeriesInformation{totalSamples,  memoryUsage,    firstTimestamp,
                                 lastTimestamp, retentionTime,  chunkCount,
                                 chunkSize,     labels,         sourceKey,
                                 rules,         duplicatePolicy};
}

// private
// static std::vector<std::string> ParseStringArray(RedisResult result) {
//     RedisResult[] redisResults = (RedisResult[])result;
//     var list = new List<std::string>();
//     if (redisResults.Length == 0) return list;
//     Array.ForEach(redisResults, str = > list.Add((string)str));
//     return list;
// }
} // namespace parser

namespace client {
inline bool timeSeriesCreate(
    sw::redis::Redis *db, const std::string &key,
    std::optional<uint64_t> retentionTime = std::nullopt,
    std::vector<TimeSeriesLabel> labels = {},
    std::optional<bool> uncompressed = std::nullopt,
    std::optional<long> chunkSizeBytes = std::nullopt,
    std::optional<command_operator::TsDuplicatePolicy> duplicatePolicy =
        std::nullopt) {
    auto args = aux::buildTsCreateArgs(key, retentionTime, labels, uncompressed,
                                       chunkSizeBytes, duplicatePolicy);
    args.insert(args.begin(), "TS.CREATE");

    return parser::parseBoolean(
        db->command<sw::redis::OptionalString>(args.begin(), args.end()));
}

inline bool
timeSeriesAlter(sw::redis::Redis *db, const std::string &key,
                std::optional<uint64_t> retentionTime = std::nullopt,
                std::vector<TimeSeriesLabel> labels = {}) {
    auto args = aux::buildTsAlterArgs(key, retentionTime, labels);
    args.insert(args.begin(), "TS.ALTER");

    return parser::parseBoolean(
        db->command<sw::redis::OptionalString>(args.begin(), args.end()));
}

inline TimeStamp timeSeriesAdd(
    sw::redis::Redis *db, const std::string &key, const TimeStamp &timestamp,
    double value, std::optional<uint64_t> retentionTime = std::nullopt,
    std::vector<TimeSeriesLabel> labels = {},
    std::optional<bool> uncompressed = std::nullopt,
    std::optional<long> chunkSizeBytes = std::nullopt,
    std::optional<command_operator::TsDuplicatePolicy> duplicatePolicy =
        std::nullopt) {
    auto args =
        aux::buildTsAddArgs(key, timestamp, value, retentionTime, labels,
                            uncompressed, chunkSizeBytes, duplicatePolicy);
    args.insert(args.begin(), "TS.ADD");

    return parser::parseTimeStamp(
        db->command<long long>(args.begin(), args.end()));
}

inline std::vector<TimeStamp> timeSeriesMAdd(
    sw::redis::Redis *db,
    const std::vector<std::tuple<std::string, TimeStamp, double>> &sequence) {
    auto args = aux::buildTsMaddArgs(sequence);
    args.insert(args.begin(), "TS.MADD");

    return parser::parseTimeStampArray(
        db->command<std::vector<long long>>(args.begin(), args.end()));
}

inline TimeStamp
timeSeriesIncrBy(sw::redis::Redis *db, const std::string &key, double value,
                 const TimeStamp &timestamp = {},
                 std::optional<uint64_t> retentionTime = std::nullopt,
                 std::vector<TimeSeriesLabel> labels = {},
                 std::optional<bool> uncompressed = std::nullopt,
                 std::optional<long> chunkSizeBytes = std::nullopt) {

    auto args =
        aux::buildTsIncrDecrByArgs(key, value, timestamp, retentionTime, labels,
                                   uncompressed, chunkSizeBytes);
    args.insert(args.begin(), "TS.INCRBY");

    return parser::parseTimeStamp(
        db->command<long long>(args.begin(), args.end()));
}

inline TimeStamp
timeSeriesDecrBy(sw::redis::Redis *db, const std::string &key, double value,
                 const TimeStamp &timestamp = {},
                 std::optional<uint64_t> retentionTime = std::nullopt,
                 std::vector<TimeSeriesLabel> labels = {},
                 std::optional<bool> uncompressed = std::nullopt,
                 std::optional<long> chunkSizeBytes = std::nullopt) {

    auto args =
        aux::buildTsIncrDecrByArgs(key, value, timestamp, retentionTime, labels,
                                   uncompressed, chunkSizeBytes);
    args.insert(args.begin(), "TS.DECRBY");

    return parser::parseTimeStamp(
        db->command<long long>(args.begin(), args.end()));
}

inline uint64_t timeSeriesDel(sw::redis::Redis *db, const std::string &key,
                              const TimeStamp &fromTimeStamp,
                              const TimeStamp &toTimeStamp) {
    auto args = aux::buildTsDelArgs(key, fromTimeStamp, toTimeStamp);
    args.insert(args.begin(), "TS.DEL");

    return parser::parseLong(db->command<long long>(args.begin(), args.end()));
}

inline bool timeSeriesCreateRule(sw::redis::Redis *db,
                                 const std::string &sourceKey,
                                 const TimeSeriesRule &rule) {
    std::vector<std::string> args{"TS.CREATERULE", sourceKey, rule.destKey()};
    if (rule.aggregation().has_value()) {
        args.push_back("AGGREGATION");
        args.push_back(command_operator::to_string(rule.aggregation().value()));
    }
    args.push_back(std::to_string(rule.timeBucket()));
    return parser::parseBoolean(
        db->command<sw::redis::OptionalString>(args.begin(), args.end()));
}

inline bool timeSeriesDeleteRule(sw::redis::Redis *db,
                                 const std::string &sourceKey,
                                 const std::string &destKey) {
    std::vector<std::string> args{"TS.DELETERULE", sourceKey, destKey};
    return parser::parseBoolean(
        db->command<sw::redis::OptionalString>(args.begin(), args.end()));
}

inline TimeSeriesTuple TimeSeriesGet(sw::redis::Redis *db,
                                     const std::string &key) {
    std::vector<std::string> args{"TS.GET", key};
    return parser::parseTimeSeriesTuple(
        db->command<std::tuple<std::string, std::string>>(args.begin(),
                                                          args.end()));
}

// inline std::vector<
//     std::tuple<std::stringstd::vector<TimeSeriesLabel>, TimeSeriesTuple>>
// TimeSeriesMGet(sw::redis::Redis *db, const std::vector<std::string> &filter,
//                std::optional<bool> withLabels = std::nullopt) {
//     auto args = aux::buildTsMgetArgs(filter, withLabels);
//     return ParseMGetesponse(db.Execute(TS.MGET, args));
// }

// inline std::vector<TimeSeriesTuple> TimeSeriesRange(sw::redis::Redis *db,
//            const std::string&key,
//             TimeStamp fromTimeStamp,
//             TimeStamp toTimeStamp,
//             long? count = null,
//             TsAggregation? aggregation = null,
//             long? timeBucket = null,
//             IReadOnlyCollection<TimeStamp> filterByTs = null,
//             (long, long)? filterByValue = null,
//             TimeStamp align = null)
//         {
//     var args =
//         BuildRangeArgs(key, fromTimeStamp, toTimeStamp, count, aggregation,
//                        timeBucket, filterByTs, filterByValue, align);
//     return ParseTimeSeriesTupleArray(db.Execute(TS.RANGE, args));
// }

// inline std::vector<TimeSeriesTuple> TimeSeriesRevRange(sw::redis::Redis *db,
//            const std::string&key,
//             TimeStamp fromTimeStamp,
//             TimeStamp toTimeStamp,
//             long? count = null,
//             TsAggregation? aggregation = null,
//             long? timeBucket = null,
//             IReadOnlyCollection<TimeStamp> filterByTs = null,
//             (long, long)? filterByValue = null,
//             TimeStamp align = null)
//         {
//     var args =
//         BuildRangeArgs(key, fromTimeStamp, toTimeStamp, count, aggregation,
//                        timeBucket, filterByTs, filterByValue, align);
//     return ParseTimeSeriesTupleArray(db.Execute(TS.REVRANGE, args));
// }

// inline std::vector<(string key, std::vector<TimeSeriesLabel> labels,
// std::vector<TimeSeriesTuple> values)> TimeSeriesMRange(sw::redis::Redis *db,
//             TimeStamp fromTimeStamp,
//             TimeStamp toTimeStamp,
//             IReadOnlyCollection<std::string> filter,
//             long? count = null,
//             TsAggregation? aggregation = null,
//             long? timeBucket = null,
//             bool? withLabels = null,
//             (string, TsReduce)? groupbyTuple = null,
//             IReadOnlyCollection<TimeStamp> filterByTs = null,
//             (long, long)? filterByValue = null,
//             IReadOnlyCollection<std::string> selectLabels = null,
//             TimeStamp align = null)
//         {
//     var args =
//         BuildMultiRangeArgs(fromTimeStamp, toTimeStamp, filter, count,
//                             aggregation, timeBucket, withLabels,
//                             groupbyTuple, filterByTs, filterByValue,
//                             selectLabels, align);
//     return ParseMRangeResponse(db.Execute(TS.MRANGE, args));
// }

// inline std::vector<(string key, std::vector<TimeSeriesLabel> labels,
// std::vector<TimeSeriesTuple> values)> TimeSeriesMRevRange(sw::redis::Redis
// *db,
//             TimeStamp fromTimeStamp,
//             TimeStamp toTimeStamp,
//             IReadOnlyCollection<std::string> filter,
//             long? count = null,
//             TsAggregation? aggregation = null,
//             long? timeBucket = null,
//             bool? withLabels = null,
//             (string, TsReduce)? groupbyTuple = null,
//             IReadOnlyCollection<TimeStamp> filterByTs = null,
//             (long, long)? filterByValue = null,
//             IReadOnlyCollection<std::string> selectLabels = null,
//             TimeStamp align = null)
//         {
//     var args =
//         BuildMultiRangeArgs(fromTimeStamp, toTimeStamp, filter, count,
//                             aggregation, timeBucket, withLabels,
//                             groupbyTuple, filterByTs, filterByValue,
//                             selectLabels, align);
//     return ParseMRangeResponse(db.Execute(TS.MREVRANGE, args));
// }

TimeSeriesInformation timeSeriesInfo(sw::redis::Redis *db,
                                     const std::string &key) {
    std::vector<std::string> args{"TS.INFO", key};
    auto reply = db->command(args.begin(), args.end());
    return parser::parseInfo(reply.get());
}

// inline std::vector<std::string>
// TimeSeriesQueryIndex(sw::redis::Redis *db,
//                      IReadOnlyCollection<std::string> filter) {
//     var args = new List<object>(filter);
//     return ParseStringArray(db.Execute(TS.QUERYINDEX, args));
// }

} // namespace client

} // namespace redis_time_series