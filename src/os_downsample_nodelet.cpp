/**
 * Copyright (c) 2022-2023, Bastian Solutions
 * All rights reserved.
 *
 * @file os_downsample_nodelet.cpp
 * @brief A nodelet to publish point clouds with reduced number of channels
 */

// prevent clang-format from altering the location of "ouster_ros/os_ros.h", the
// header file needs to be the first include due to PCL_NO_PRECOMPILE flag
// clang-format off
#include "ouster_ros/os_ros.h"
// clang-format on

#include <boost/bind.hpp>
#include <nodelet/nodelet.h>
#include <pluginlib/class_list_macros.h>
#include <ros/ros.h>
#include <sensor_msgs/PointCloud2.h>

#include <algorithm>
#include <cmath>
#include <functional>

#include "ouster_ros/GetMetadata.h"

namespace sensor = ouster::sensor;
using sensor::UDPProfileLidar;

namespace ouster_ros {
class OusterDownsample : public nodelet::Nodelet {
   private:
    virtual void onInit() override {
        auto& pnh = getPrivateNodeHandle();
        num_channels = static_cast<size_t>(pnh.param("num_channels", 32));
        beam_config = pnh.param("beam_configuration", std::string("uniform"));

        auto& nh = getNodeHandle();
        ouster_ros::GetMetadata metadata{};
        auto client = nh.serviceClient<ouster_ros::GetMetadata>("get_metadata");
        client.waitForExistence();
        if (!client.call(metadata)) {
            auto error_msg = "OusterDownsample: Calling get_metadata service "
                "failed";
            NODELET_ERROR_STREAM(error_msg);
            throw std::runtime_error(error_msg);
        }

        NODELET_INFO("OusterDownsample: retrieved sensor metadata!");

        auto info = sensor::parse_metadata(metadata.response.metadata);
        auto n_returns = info.format.udp_profile_lidar ==
                            UDPProfileLidar::PROFILE_RNG19_RFL8_SIG16_NIR16_DUAL
                        ? 2
                        : 1;

        NODELET_INFO_STREAM("Profile has " << n_returns << " return(s)");

        auto img_suffix = [](int ind) {
            if (ind == 0) return std::string();
            return std::to_string(ind + 1);  // need second return to return 2
        };

        lidar_pubs.resize(n_returns);
        for (int ii = 0; ii < n_returns; ++ii) {
            auto pub = nh.advertise<sensor_msgs::PointCloud2>(
                beam_config + std::string("_") + std::to_string(num_channels)
                + std::string("/points") + img_suffix(ii), 10);
            lidar_pubs.at(ii) = pub;
        }

        lidar_subs.resize(n_returns);
        for (int ii = 0; ii < n_returns; ++ii) {
            auto sub = nh.subscribe<sensor_msgs::PointCloud2>(
                std::string("points") + img_suffix(ii),
                10,
                boost::bind(&OusterDownsample::lidar_downsample_channels,
                          this,
                          _1,
                          lidar_pubs.at(ii)));
            lidar_subs.at(ii) = sub;
        }
    }

    void lidar_downsample_channels(const sensor_msgs::PointCloud2::ConstPtr&
                                   msg_ptr,
                                   ros::Publisher& pub) {
        if(num_channels >= msg_ptr->height) {
            NODELET_ERROR_STREAM_THROTTLE(5, "Invalid down sampling! Requested "
                << "output channels (" << num_channels << ") is not less than "
                << "input channels (" << msg_ptr->height << ").");
            return;
        }

        sensor_msgs::PointCloud2 msg{};
        msg.header = msg_ptr->header;
        msg.height = num_channels;
        msg.width = msg_ptr->width;
        msg.fields = msg_ptr->fields;
        msg.is_bigendian = msg_ptr->is_bigendian;
        msg.point_step = msg_ptr->point_step;
        msg.row_step = msg_ptr->row_step;
        msg.is_dense = msg_ptr->is_dense;
        msg.data.resize(num_channels * msg.row_step);

        // Use static local initialization to generate ring indices and
        // calculate ring value offset once and cache results
        static auto ring_idxs = generate_downsample_indices(beam_config,
                                                            msg_ptr->height,
                                                            num_channels);

        static auto ring_offset =
            std::find_if(msg.fields.begin(),
                         msg.fields.end(),
                         [](const sensor_msgs::PointField& f)
                         {
                           return f.name == "ring";
                         })->offset;

        // Copy ring data
        for(size_t ii = 0; ii < num_channels; ++ii) {
            auto from_offset_begin = ring_idxs.at(ii) * msg_ptr->row_step;
            auto from_offset_end = from_offset_begin
                                   + msg_ptr->width * msg_ptr->point_step;
            auto to_offset = ii * msg.row_step;
            std::copy(msg_ptr->data.begin() + from_offset_begin,
                      msg_ptr->data.begin() + from_offset_end,
                      msg.data.begin() + to_offset);

            uint8_t ring_val = static_cast<uint8_t>(ii);
            // Overwrite ring values
            for(size_t jj = 0; jj < msg.width; ++jj) {
                auto offset = to_offset + jj * msg.point_step + ring_offset;
                memcpy(&msg.data.at(offset), &ring_val, sizeof(uint8_t));
            }
        }

        pub.publish(msg);
    }

    std::vector<size_t> generate_downsample_indices(const std::string& config,
                                                    const size_t in_channels,
                                                    const size_t channels) {
        std::vector<size_t> idxs;
        if(config == "uniform") {
            idxs = generate_uniform_indices(in_channels, channels);
        }
        else if(config == "gradient") {
            idxs = generate_gradient_indices(in_channels, channels);
        }
        else {

        }

        return idxs;
    }

    std::vector<size_t> generate_uniform_indices(const size_t in_channels,
                                                 const size_t channels) {
        size_t step = in_channels / channels;  // Floor division

        std::vector<size_t> idxs(channels);
        for(size_t ii = 0; ii < channels; ++ii) {
            idxs.at(ii) = step * ii;
        }

        return idxs;
    }

    std::vector<size_t> generate_gradient_indices(const size_t in_channels,
                                                  const size_t channels) {
        // Simple piece-wise triangular gradient formula with cross-over at
        // channels / 2
        double m = in_channels;
        double n = channels;
        double slope = (4 / n) * ((m / n) - 1);
        double b1 = 2 * ((m / n) - 1) + 1;  // @ x = 0
        double b2 = 1;  // @ x = n / 2

        size_t m_half_idx = std::floor((m - 1) / 2);  // Index of m_half
        size_t n_half_idx = std::floor((n - 1) / 2);  // Index of n_half
        size_t prev_idx = m_half_idx;

        std::vector<size_t> idxs(static_cast<size_t>(n));
        idxs.at(n_half_idx) = prev_idx;

        // Populate first half of indices in reverse order
        for(int ii = n_half_idx - 1; ii >= 0; --ii) {
            size_t step = static_cast<size_t>(std::round(-slope * ii + b1));
            // Cast to int to prevent unsigned rollover
            idxs.at(ii) = std::max(static_cast<int>(prev_idx)
                                   - static_cast<int>(step), 0);
            prev_idx = idxs.at(ii);
        }

        // Populate second half of indices
        prev_idx = m_half_idx;  // Reset initial index
        for(int ii = n_half_idx + 1; ii < n; ++ii) {
            size_t step = static_cast<size_t>(std::round(slope * (ii - n_half_idx) + b2));
            idxs.at(ii) = std::min(prev_idx + step, static_cast<size_t>(m - 1));
            prev_idx = idxs.at(ii);
        }

        return idxs;
    }

   private:
    std::vector<ros::Subscriber> lidar_subs;
    std::vector<ros::Publisher> lidar_pubs;

    size_t num_channels;
    std::string beam_config;
};
}  // namespace ouster_ros

PLUGINLIB_EXPORT_CLASS(ouster_ros::OusterDownsample, nodelet::Nodelet)
