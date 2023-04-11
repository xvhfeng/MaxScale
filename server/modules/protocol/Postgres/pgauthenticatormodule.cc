/*
 * Copyright (c) 2023 MariaDB plc
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-02-21
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#include "pgauthenticatormodule.hh"

std::string PgAuthenticatorModule::supported_protocol() const
{
    return MXB_MODULE_NAME;
}

bool AuthIdEntry::operator==(const AuthIdEntry& rhs) const
{
    return name == rhs.name && password == rhs.password && super == rhs.super && inherit == rhs.inherit
           && can_login == rhs.can_login;
}
