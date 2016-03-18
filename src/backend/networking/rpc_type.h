//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_type.h
//
// Identification: /peloton/src/backend/networking/rpc_type.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace peloton {
namespace networking {

enum MessageType {
    MSG_TYPE_INVALID = 0,
    MSG_TYPE_REQ,
    MSG_TYPE_REP
};

} // namespace message
} // namespace peloton
