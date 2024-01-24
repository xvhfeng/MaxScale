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
 * @file include/maxscale/filter.hh - The public filter interface
 */

#include <maxscale/ccdefs.hh>
#include <stdint.h>
#include <maxscale/parser.hh>
#include <maxscale/routing.hh>
#include <maxscale/protocol2.hh>

struct json_t;
class GWBUF;
class MXS_SESSION;
class SERVICE;
namespace maxscale
{
class ProtocolData;
class ConfigParameters;
class FilterSession;
namespace config
{
class Configuration;
}
}

namespace maxscale
{

class Parser;

/**
 * Filter is the base class of all filters.
 */
struct Filter
{
    virtual ~Filter() = default;

    /**
     * Called to create a new user session within the filter
     *
     * This function is called when a new filter session is created for a client.
     *
     * @param session  Client MXS_SESSION object
     * @param service  The service in which this filter session is created
     *
     * @return New filter session or NULL on error
     */
    virtual mxs::FilterSession* newSession(MXS_SESSION* session, SERVICE* service) = 0;

    /**
     * @brief Called for diagnostic output
     *
     * @param instance Filter instance
     * @param fsession Filter session, NULL if general information about the filter is queried
     *
     * @return JSON formatted information about the filter
     *
     * @see jansson.h
     */
    virtual json_t* diagnostics() const = 0;

    /**
     * @brief Called to obtain the capabilities of the filter
     *
     * @return Zero or more bitwise-or'd values from the mxs_routing_capability_t enum
     *
     * @see routing.hh
     */
    virtual uint64_t getCapabilities() const = 0;

    /**
     * Get the configuration of a filter instance
     *
     * The configure method of the returned configuration will be called after the initial creation of the
     * filter as well as any time a parameter is modified at runtime.
     *
     * @return The configuration for the filter instance
     */
    virtual mxs::config::Configuration& getConfiguration() = 0;

    /**
     * Get the set of supported protocols
     *
     * @return The names of the protocols supported by this filter. If the filter is protocol-agnostic,
     *         the constant MXS_ANY_PROTOCOL can be used.
     */
    virtual std::set<std::string> protocols() const = 0;
};

/**
 * @class FilterSession filter.hh <maxscale/filter.hh>
 *
 * FilterSession is a base class for filter sessions. A concrete filter session
 * class should be derived from this class and override all relevant functions.
 *
 * Note that even though this class is intended to be derived from, no functions
 * are virtual. That is by design, as the class will be used in a context where
 * the concrete class is known. That is, there is no need for the virtual mechanism.
 */
class FilterSession : public mxs::Routable
{
public:
    /**
     * The FilterSession instance will be deleted when a client session
     * has terminated. Will be called only after @c close() has been called.
     */
    virtual ~FilterSession();

    /**
     * Called for setting the component following this filter session.
     *
     * @param down The component following this filter.
     */
    void setDownstream(mxs::Routable* down);

    /**
     * Called for setting the component preceeding this filter session.
     *
     * @param up The component preceeding this filter.
     */
    void setUpstream(mxs::Routable* up);

    /**
     * Called when a packet being is routed to the backend. The filter should
     * forward the packet to the downstream component.
     *
     * @param pPacket A client packet.
     *
     * @return True for success, false for error. If the function return false, a call to the parent
     *         component's handleError method is made.
     *
     * @throws May throw mxb::Exception to abort the routing. If the exception is thrown, a call to the parent
     *         component's handleError method is made.
     */
    bool routeQuery(GWBUF&& packet) override;

    /**
     * Called when a packet is routed to the client. The filter should
     * forward the packet to the upstream component.
     *
     * @param pPacket A client packet.
     * @param down    The downstream components where the response came from
     * @param reply   The reply information (@see target.hh)
     *
     * @return True for success, false for error. If the function returns false, the session will be closed.
     *
     * @throws May throw mxb::Exception. If the exception is thrown, the exception message is logged and the
     *         session will be closed.
     */
    bool clientReply(GWBUF&& packet, const mxs::ReplyRoute& down, const mxs::Reply& reply) override;

    /**
     * Called for obtaining diagnostics about the filter session.
     */
    json_t* diagnostics() const;

protected:
    FilterSession(MXS_SESSION* pSession, SERVICE* service);

    /**
     * To be called by a filter that short-circuits the request processing.
     * If this function is called (in routeQuery), the filter must return
     * without passing the request further.
     *
     * @param pResponse  The response to be sent to the client.
     * @param pTarget    The source of the response
     */
    void set_response(GWBUF&& response) const;

    /**
     * Returns a parser appropriate for the protocol of this session's client
     * connection. This function must only be called if it is know, due to the
     * context where it is called, that there will be a parser.
     *
     * @return The parser associated with the protocol of this session's client connection.
     */
    const Parser& parser() const
    {
        return const_cast<FilterSession*>(this)->parser();
    }

    Parser& parser()
    {
        mxb_assert_message(m_pParser, "Protocol of client connection does not have a parser.");
        return *m_pParser;
    }

    /**
     * @return The SQL of @c packet, or an empty string if it does not contain SQL.
     */
    std::string_view get_sql(const GWBUF& stmt) const
    {
        return parser().get_sql(stmt);
    }

    // TODO: To be removed when everyone can handle string_views.
    std::string get_sql_string(const GWBUF& stmt) const
    {
        return std::string {get_sql(stmt)};
    }

    /**
     * Get the protocol data for this session
     *
     * @return The protocol data of the protocol
     */
    const mxs::ProtocolData& protocol_data() const;

    /**
     * The protocol used by this filter session
     *
     * @return The protocol module being used
     */
    const mxs::ProtocolModule& protocol() const;

protected:
    MXS_SESSION* m_pSession;/*< The MXS_SESSION this filter session is associated with. */
    SERVICE*     m_pService;/*< The service for which this session was created. */
    Parser*      m_pParser; /*< The parser suitable the protocol of this filter. */

    mxs::Routable* m_down = (mxs::Routable*)BAD_ADDR;   /*< The downstream component. */
    mxs::Routable* m_up = (mxs::Routable*)BAD_ADDR;     /*< The upstream component. */
};

/**
 * Filter module call api.
 */
struct FILTER_API
{
    /**
     * @brief Create a new instance of the filter
     *
     * This function is called when a new filter instance is created. The return
     * value of this function will be passed as the first parameter to the
     * other API functions.
     *
     * @param name    Name of the filter instance
     *
     * @return New filter instance on NULL on error
     */
    Filter* (* createInstance)(const char* name);
};
}

/**
 * The filter API version. If the MXS_FILTER_OBJECT structure or the filter API
 * is changed these values must be updated in line with the rules in the
 * file modinfo.h.
 */
// TODO: Update this from 4.0.0 to 5.0.0 for 2.6
#define MXS_FILTER_VERSION {4, 0, 0}



namespace maxscale
{
template<class FilterClass>
class FilterApi
{
public:
    FilterApi() = delete;
    FilterApi(const FilterApi&) = delete;
    FilterApi& operator=(const FilterApi&) = delete;

    static Filter* createInstance(const char* name)
    {
        Filter* inst = nullptr;
        MXS_EXCEPTION_GUARD(inst = FilterClass::create(name));
        return inst;
    }

    static FILTER_API s_api;
};

template<class FilterClass>
FILTER_API FilterApi<FilterClass>::s_api =
{
    &FilterApi<FilterClass>::createInstance,
};
}

/**
 * MXS_FILTER_DEF represents a filter definition from the configuration file.
 * Its exact definition is private to MaxScale.
 */
class MXS_FILTER_DEF
{
};

/**
 * Get the filter instance of a particular filter definition.
 *
 * @return A filter instance.
 */
mxs::Filter* filter_def_get_instance(const MXS_FILTER_DEF* filter_def);
