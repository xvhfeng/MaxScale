/*
 * Copyright (c) 2018 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-11-30
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
#pragma once

/**
 * @file routing.hh - Common definitions and declarations for routers and filters.
 */

#include <maxscale/ccdefs.hh>
#include <maxscale/target.hh>

namespace maxscale
{
/**
 * mxs::Routable is the base type representing the session related data of a particular routing module
 * instance. Implemented by filter and router sessions.
 */
class Routable
{
public:
    virtual ~Routable() = default;

    /**
     * Called when a packet is traveling downstream, towards a backend.
     *
     * @param pPacket Packet to route
     *
     * @return True for success, false for error. If the function return false, a call to the parent
     *         component's handleError method is made.
     *
     * @throws May throw mxb::Exception to abort the routing. If the exception is thrown, a call to the parent
     *         component's handleError method is made.
     */
    virtual bool routeQuery(GWBUF&& packet) = 0;

    /**
     * Called when a packet is traveling upstream, towards the client.
     *
     * @param pPacket Packet to route
     * @param down Response source
     * @param reply Reply information
     *
     * @return True for success, false for error. If the function returns false, the session will be closed.
     *
     * @throws May throw mxb::Exception. If the exception is thrown, the exception message is logged and the
     *         session will be closed.
     */
    virtual bool clientReply(GWBUF&& packet, const mxs::ReplyRoute& down, const mxs::Reply& reply) = 0;

    /**
     * Set the Endpoint that this Routable is a part of
     *
     * @param endpoint The Endpoint for this Routable
     */
    void setEndpoint(mxs::Endpoint* endpoint)
    {
        m_endpoint = endpoint;
    }

    /**
     * Get the Endpoint of this Routable
     *
     * @return The Endpoint of this Routable
     */
    const mxs::Endpoint& endpoint() const
    {
        mxb_assert(m_endpoint);
        return *m_endpoint;
    }

protected:
    mxs::Endpoint* m_endpoint = nullptr;
};
}

/**
 * Routing capability type. Indicates what kind of input a router or
 * a filter accepts.
 *
 *       The capability bit ranges are:
 *           0-15:  general capability bits
 *           16-23: router specific bits
 *           24-31: filter specific bits
 *           32-39: authenticator specific bits
 *           40-47: protocol specific bits
 *           48-55: monitor specific bits
 *           56-63: reserved for future use
 *
 * @note The values of the capabilities here *must* be between 0x0000
 *       and 0x8000, that is, bits 0 to 15.
 */
enum mxs_routing_capability_t
{
    /**
     * routeQuery is called with one packet per buffer (currently always on). The buffer is always contiguous.
     *
     * Deprecated: routeQuery is always called with a complete packet, this is a redundant option.
     *
     * Binary: 0b0000000000000001
     */
    RCAP_TYPE_STMT_INPUT = (1 << 0),

    /**
     * The transaction state and autocommit mode of the session are tracked; implies RCAP_TYPE_STMT_INPUT.
     *
     * Deprecated: transaction tracking is now always done, this is a redundant option.
     *
     * Binary: 0b0000000000000011
     */
    RCAP_TYPE_TRANSACTION_TRACKING = (1 << 1) | RCAP_TYPE_STMT_INPUT,

    /**
     * Results are delivered as a set of complete packets. The buffer passed to clientReply can contain
     * multiple packets.
     *
     * Binary: 0b0000000000000100
     */
    RCAP_TYPE_PACKET_OUTPUT = (1 << 2),

    /**
     * Request and response tracking: tells when a response to a query is complete. Implies
     * RCAP_TYPE_STMT_INPUT and RCAP_TYPE_PACKET_OUTPUT.
     *
     * Deprecated: request tracking is now always done, this is a redundant option.
     *
     * Binary: 0b0000000000001101
     */
    RCAP_TYPE_REQUEST_TRACKING = (1 << 3) | RCAP_TYPE_STMT_INPUT | RCAP_TYPE_PACKET_OUTPUT,

    /**
     * clientReply is called with one packet per buffer. The buffer is always contiguous. Implies
     * RCAP_TYPE_PACKET_OUTPUT.
     *
     * Binary: 0b0000000000010100
     */
    RCAP_TYPE_STMT_OUTPUT = (1 << 4) | RCAP_TYPE_PACKET_OUTPUT,

    /**
     * All result are delivered in one buffer. Implies RCAP_TYPE_REQUEST_TRACKING.
     *
     * Binary: 0b0000000000101101
     */
    RCAP_TYPE_RESULTSET_OUTPUT = (1 << 5) | RCAP_TYPE_REQUEST_TRACKING,

    /**
     * Track session state changes; implies RCAP_TYPE_PACKET_OUTPUT
     *
     * Binary: 0b0000000001000100
     */
    RCAP_TYPE_SESSION_STATE_TRACKING = (1 << 6) | RCAP_TYPE_PACKET_OUTPUT,

    /**
     * Query classification is always done. This lets the protocol module know that at least one module in the
     * routing chain will do query classification on each query. This allows some optimizations to be done
     * that skip some of the custom mini-parsers for the majority of commands.
     *
     * Binary: 0b0000000010000000
     */
    RCAP_TYPE_QUERY_CLASSIFICATION = (1 << 7),

    /**
     * Track modifications to the session state and automatically restore them whenever a reconnection occurs.
     * This capability must be declared by the router in order for it to be able to safely reconnect
     * mid-session.
     *
     * Binary: 0b0000000100000000
     */
    RCAP_TYPE_SESCMD_HISTORY = (1 << 8),

    /**
     * Disables all new protocol extensions. This currently includes the metadata caching extension that was
     * added to MariaDB in 10.6.
     *
     * Binary: 0b0000001000000000
     */
    RCAP_TYPE_OLD_PROTOCOL = (1 << 9),

    /**
     * Force connection to use multi-statements and multi-results
     *
     * Binary: 0b0000010000000000
     */
    RCAP_TYPE_MULTI_STMT_SQL = (1 << 10),

    /**
     * The module (router or filter) cannot handle a change in the number of threads.
     *
     * Binary: 0b0000100000000000
     */
    RCAP_TYPE_NO_THREAD_CHANGE = (1 << 11),
};

#define RCAP_TYPE_NONE 0

/**
 * Determines whether a particular capability type is required.
 *
 * @param capabilites The capability bits to be tested.
 * @param type        A particular capability type or a bitmask of types.
 *
 * @return True, if @c type is present in @c capabilities.
 */
static inline bool rcap_type_required(uint64_t capabilities, uint64_t type)
{
    return (capabilities & type) == type;
}
