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

#include "cat.hh"
#include "catsession.hh"
#include <maxscale/service.hh>

using namespace maxscale;

namespace
{
mxs::config::Specification s_spec(MXB_MODULE_NAME, mxs::config::Specification::ROUTER);
}

Cat::Cat(const std::string& name)
    : m_config(name, &s_spec)
{
}

Cat* Cat::create(SERVICE* pService)
{
    return new Cat(pService->name());
}

mxs::RouterSession* Cat::newSession(MXS_SESSION* pSession, const Endpoints& endpoints)
{
    auto backends = RWBackend::from_endpoints(endpoints);
    bool connected = false;

    for (auto& a : backends)
    {
        if (a.can_connect())
        {
            try
            {
                a.connect();
                connected = true;
            }
            catch (const mxb::Exception& e)
            {
                MXB_INFO("Failed to connect to '%s': %s", a.name(), e.what());
            }
        }
    }

    if (!connected)
    {
        throw mxb::Exception("Failed to connect to any backends");
    }

    return new CatSession(pSession, this, std::move(backends));
}

json_t* Cat::diagnostics() const
{
    return NULL;
}

const uint64_t caps = RCAP_TYPE_REQUEST_TRACKING | RCAP_TYPE_STMT_OUTPUT | RCAP_TYPE_STMT_INPUT;

uint64_t Cat::getCapabilities() const
{
    return caps;
}

/**
 * The module entry point routine. It is this routine that
 * must populate the structure that is referred to as the
 * "module object", this is a structure with the set of
 * external entry points for this module.
 *
 * @return The module object
 */
extern "C" MXS_MODULE* MXS_CREATE_MODULE()
{
    static MXS_MODULE info =
    {
        mxs::MODULE_INFO_VERSION,
        "cat",
        mxs::ModuleType::ROUTER,
        mxs::ModuleStatus::ALPHA,
        MXS_ROUTER_VERSION,
        "Resultset concatenation router",
        "V1.0.0",
        caps,
        &mxs::RouterApi<Cat>::s_api,
        NULL,   /* Process init. */
        NULL,   /* Process finish. */
        NULL,   /* Thread init. */
        NULL,   /* Thread finish. */
        &s_spec
    };

    return &info;
}
