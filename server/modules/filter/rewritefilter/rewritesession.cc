/*
 * Copyright (c) 2022 MariaDB Corporation Ab
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl.
 *
 * Change Date: 2026-06-06
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#define MXB_MODULE_NAME "rewitefilter"
#include "rewritesession.hh"
#include "rewritefilter.hh"

RewriteFilterSession::RewriteFilterSession(MXS_SESSION* pSession,
                                           SERVICE* pService,
                                           const std::shared_ptr<Settings>& sSettings)
    : maxscale::FilterSession(pSession, pService)
    , m_sSettings(sSettings)
{
}

RewriteFilterSession::~RewriteFilterSession()
{
}

// static
RewriteFilterSession* RewriteFilterSession::create(MXS_SESSION* pSession,
                                                   SERVICE* pService,
                                                   const std::shared_ptr<Settings>& sSettings)
{
    return new RewriteFilterSession(pSession, pService, sSettings);
}
