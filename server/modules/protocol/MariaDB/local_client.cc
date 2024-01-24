/*
 * Copyright (c) 2016 MariaDB Corporation Ab
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

#include <maxscale/protocol/mariadb/local_client.hh>
#include <maxscale/routingworker.hh>

LocalClient::~LocalClient()
{
    if (m_down && m_down->is_open())
    {
        m_down->close();
    }
}

bool LocalClient::queue_query(GWBUF&& buffer)
{
    bool rval = false;

    if (m_down->is_open())
    {
        rval = m_down->routeQuery(std::move(buffer));
    }


    return rval;
}

std::unique_ptr<LocalClient> LocalClient::create(MXS_SESSION* session, mxs::Target* target)
{
    std::unique_ptr<LocalClient> relay;
    auto state = session->state();

    if (state == MXS_SESSION::State::STARTED || state == MXS_SESSION::State::CREATED)
    {
        relay.reset(new LocalClient(session, target));
    }

    return relay;
}

void LocalClient::connect()
{
    m_down->connect();
}

bool LocalClient::routeQuery(GWBUF&& buffer)
{
    mxb_assert(!true);
    return false;
}

bool LocalClient::clientReply(GWBUF&& buffer, const mxs::ReplyRoute& down, const mxs::Reply& reply)
{
    if (m_cb)
    {
        m_cb(std::move(buffer), down, reply);
    }

    return true;
}

bool LocalClient::handleError(mxs::ErrorType type, const std::string& error, mxs::Endpoint* down, const mxs::Reply& reply)
{
    if (m_down->is_open())
    {
        if (m_err)
        {
            m_err(error, down->target(), reply);
        }

        m_down->close();
    }

    return true;
}
