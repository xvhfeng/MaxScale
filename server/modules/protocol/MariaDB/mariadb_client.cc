/*
 *
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

#include <maxscale/protocol/mariadb/module_names.hh>
#define MXB_MODULE_NAME MXS_MARIADB_PROTOCOL_NAME

#include <maxscale/protocol/mariadb/client_connection.hh>

#include <inttypes.h>
#include <limits.h>
#include <netinet/tcp.h>
#include <sys/stat.h>
#include <algorithm>
#include <string>
#include <vector>
#include <grp.h>
#include <pwd.h>
#include <utility>

#include <maxbase/proxy_protocol.hh>
#include <maxbase/format.hh>
#include <maxscale/event.hh>
#include <maxscale/listener.hh>
#include <maxscale/modinfo.hh>
#include <maxscale/protocol.hh>
#include <maxscale/protocol/mariadb/authenticator.hh>
#include <maxscale/protocol/mariadb/backend_connection.hh>
#include <maxscale/protocol/mariadb/local_client.hh>
#include <maxscale/protocol/mariadb/mariadbparser.hh>
#include <maxscale/protocol/mariadb/mysql.hh>
#include <maxscale/router.hh>
#include <maxscale/routingworker.hh>
#include <maxscale/session.hh>
#include <maxscale/ssl.hh>
#include <maxscale/threadpool.hh>
#include <maxscale/version.hh>
#include <maxsql/mariadb.hh>

#include "detect_special_query.hh"
#include "packet_parser.hh"
#include "setparser.hh"
#include "sqlmodeparser.hh"
#include "user_data.hh"

namespace
{
using AuthRes = mariadb::ClientAuthenticator::AuthRes;
using ExcRes = mariadb::ClientAuthenticator::ExchRes;
using UserEntryType = mariadb::UserEntryType;
using TrxState = mariadb::TrxTracker::TrxState;
using std::move;
using std::string;

const string base_plugin = DEFAULT_MYSQL_AUTH_PLUGIN;
const mxs::ListenerData::UserCreds default_mapped_creds = {"", base_plugin};
const int CLIENT_CAPABILITIES_LEN = 32;
const int SSL_REQUEST_PACKET_SIZE = MYSQL_HEADER_LEN + CLIENT_CAPABILITIES_LEN;
const int NORMAL_HS_RESP_MIN_SIZE = MYSQL_AUTH_PACKET_BASE_SIZE + 2;
const int NORMAL_HS_RESP_MAX_SIZE = MYSQL_PACKET_LENGTH_MAX - 1;

const int ER_OUT_OF_ORDER = 1156;
const char PACKETS_OOO_MSG[] = "Got packets out of order";      // Matches server message
const char WRONG_SEQ_FMT[] = "Client (%s) sent packet with unexpected sequence number. Expected %i, got %i.";
const int ER_BAD_HANDSHAKE = 1043;
const char BAD_HANDSHAKE_MSG[] = "Bad handshake";   // Matches server message
const char BAD_HANDSHAKE_FMT[] = "Client (%s) sent an invalid HandshakeResponse.";
// MaxScale-specific message. Possibly useful for clarifying that MaxScale is expecting SSL connection.
const char BAD_SSL_HANDSHAKE_MSG[] = "Bad SSL handshake";
const char BAD_SSL_HANDSHAKE_FMT[] = "Client (%s) sent an invalid SSLRequest.";
const char HANDSHAKE_ERRSTATE[] = "08S01";

// The past-the-end value for the session command IDs we generate (includes prepared statements). When this ID
// value is reached, the counter is reset back to 1. This makes sure we reserve the values 0 and 0xffffffff as
// special values that are never assigned by MaxScale.
const uint32_t MAX_SESCMD_ID = std::numeric_limits<uint32_t>::max();
static_assert(MAX_SESCMD_ID == MARIADB_PS_DIRECT_EXEC_ID);

// Default version string sent to clients
const string default_version = string("5.5.5-10.4.32 ") + MAXSCALE_VERSION + "-maxscale";

class ThisUnit
{
public:
    mxb::Regex special_queries_regex;
};
ThisUnit this_unit;

string get_version_string(SERVICE* service)
{
    string service_vrs = service->version_string();

    if (service_vrs.empty())
    {
        service_vrs = default_version + service->custom_version_suffix();
    }

    return service_vrs;
}

enum class CapTypes
{
    XPAND,      // XPand, doesn't include SESSION_TRACK as it doesn't support it
    NORMAL,     // The normal capabilities but without the extra MariaDB-only bits
    MARIADB,    // All capabilities
};

// Returns the capability type, version number and the capabilities themselves
std::tuple<CapTypes, uint64_t, uint64_t> get_supported_cap_types(SERVICE* service)
{
    uint64_t caps = GW_MYSQL_CAPABILITIES_SERVER;
    CapTypes type = CapTypes::MARIADB;
    uint64_t version = std::numeric_limits<uint64_t>::max();

    for (SERVER* s : service->reachable_servers())
    {
        const auto& info = s->info();

        if (info.type() != SERVER::VersionInfo::Type::UNKNOWN)
        {
            caps &= info.capabilities();
        }

        if (info.type() == SERVER::VersionInfo::Type::XPAND)
        {
            // At least one node is XPand and since it's the most restrictive, we can return early.
            type = CapTypes::XPAND;
            break;
        }
        else
        {
            version = std::min(info.version_num().total, version);

            if (version < 100200)
            {
                type = CapTypes::NORMAL;
            }
        }
    }

    return {type, version, caps};
}

bool call_getpwnam_r(const char* user, gid_t& group_id_out)
{
    bool rval = false;
    // getpwnam_r requires a buffer for result data. The size is not known beforehand. Guess the size and
    // try again with a larger buffer if necessary.
    int buf_size = 1024;
    const int buf_size_limit = 1024000;
    const char err_msg[] = "'getpwnam_r' on '%s' failed. Error %i: %s";
    string buffer;
    passwd output {};
    passwd* output_ptr = nullptr;
    bool keep_trying = true;

    while (buf_size <= buf_size_limit && keep_trying)
    {
        keep_trying = false;
        buffer.resize(buf_size);
        int ret = getpwnam_r(user, &output, &buffer[0], buffer.size(), &output_ptr);

        if (output_ptr)
        {
            group_id_out = output_ptr->pw_gid;
            rval = true;
        }
        else if (ret == 0)
        {
            // No entry found, likely the user is not a Linux user.
            MXB_INFO("Tried to check groups of user '%s', but it is not a Linux user.", user);
        }
        else if (ret == ERANGE)
        {
            // Buffer was too small. Try again with a larger one.
            buf_size *= 10;
            if (buf_size > buf_size_limit)
            {
                MXB_ERROR(err_msg, user, ret, mxb_strerror(ret));
            }
            else
            {
                keep_trying = true;
            }
        }
        else
        {
            MXB_ERROR(err_msg, user, ret, mxb_strerror(ret));
        }
    }
    return rval;
}

bool call_getgrgid_r(gid_t group_id, string& name_out)
{
    bool rval = false;
    // getgrgid_r requires a buffer for result data. The size is not known beforehand. Guess the size and
    // try again with a larger buffer if necessary.
    int buf_size = 1024;
    const int buf_size_limit = 1024000;
    const char err_msg[] = "'getgrgid_r' on %ui failed. Error %i: %s";
    string buffer;
    group output {};
    group* output_ptr = nullptr;
    bool keep_trying = true;

    while (buf_size <= buf_size_limit && keep_trying)
    {
        keep_trying = false;
        buffer.resize(buf_size);
        int ret = getgrgid_r(group_id, &output, &buffer[0], buffer.size(), &output_ptr);

        if (output_ptr)
        {
            name_out = output_ptr->gr_name;
            rval = true;
        }
        else if (ret == 0)
        {
            MXB_ERROR("Group id %ui is not a valid Linux group.", group_id);
        }
        else if (ret == ERANGE)
        {
            // Buffer was too small. Try again with a larger one.
            buf_size *= 10;
            if (buf_size > buf_size_limit)
            {
                MXB_ERROR(err_msg, group_id, ret, mxb_strerror(ret));
            }
            else
            {
                keep_trying = true;
            }
        }
        else
        {
            MXB_ERROR(err_msg, group_id, ret, mxb_strerror(ret));
        }
    }
    return rval;
}

std::string attr_to_str(const std::vector<uint8_t>& data)
{
    if (data.empty())
    {
        return "no attributes";
    }

    const uint8_t* ptr = data.data();
    const uint64_t len = mxq::leint_consume((uint8_t**)&ptr);
    const uint8_t* end = ptr + len;
    std::string values;

    while (ptr < end)
    {
        size_t key_size;
        const char* key = mxq::lestr_consume_safe(&ptr, end, &key_size);

        if (!key)
        {
            break;
        }

        size_t value_size;
        const char* value = mxq::lestr_consume_safe(&ptr, end, &value_size);

        if (!value)
        {
            break;
        }

        values.append(key, key_size);
        values.append("=");
        values.append(value, value_size);
        values.append(" ");
    }

    return values;
}

json_t* attr_to_json(const std::vector<uint8_t>& data)
{
    if (data.empty())
    {
        return json_null();
    }

    const uint8_t* ptr = data.data();
    const uint64_t len = mxq::leint_consume((uint8_t**)&ptr);
    const uint8_t* end = ptr + len;
    json_t* js = json_object();

    while (ptr < end)
    {
        size_t key_size;
        const char* key = mxq::lestr_consume_safe(&ptr, end, &key_size);

        if (!key)
        {
            break;
        }

        size_t value_size;
        const char* value = mxq::lestr_consume_safe(&ptr, end, &value_size);

        if (!value)
        {
            break;
        }

        json_object_set_new(js, std::string(key, key_size).c_str(), json_stringn(value, value_size));
    }

    return js;
}
}

struct KillInfo
{
    KillInfo(std::string query, MXS_SESSION* ses)
        : origin(mxs::RoutingWorker::get_current())
        , session(ses)
        , query_base(std::move(query))
    {
    }

    virtual ~KillInfo() = default;
    virtual void        generate_target_list(mxs::RoutingWorker* worker) = 0;
    virtual std::string generate_kill_query(SERVER* target_server) = 0;

    mxs::RoutingWorker* origin;
    MXS_SESSION*        session;
    std::string         query_base;
    std::mutex          targets_lock;
    std::set<SERVER*>   targets;
};

struct ConnKillInfo : public KillInfo
{
    ConnKillInfo(uint64_t id, std::string query, MXS_SESSION* ses)
        : KillInfo(std::move(query), ses)
        , target_ses_id(id)
    {
    }

    void        generate_target_list(mxs::RoutingWorker* worker) override;
    std::string generate_kill_query(SERVER* target_server) override;

    uint64_t                    target_ses_id;
    std::map<SERVER*, uint64_t> be_thread_ids;
};

struct UserKillInfo : public KillInfo
{
    UserKillInfo(std::string name, std::string query, MXS_SESSION* ses)
        : KillInfo(std::move(query), ses)
        , user(std::move(name))
    {
    }

    void        generate_target_list(mxs::RoutingWorker* worker) override;
    std::string generate_kill_query(SERVER* target_server) override;

    std::string user;
};

void ConnKillInfo::generate_target_list(mxs::RoutingWorker* worker)
{
    const auto& sessions = worker->session_registry();
    auto* session = sessions.lookup(target_ses_id);
    if (session)
    {
        // In theory, a client could issue a KILL-command on a non-MariaDB session (perhaps on purpose!).
        // Limit killing to MariaDB sessions only.
        if (session->protocol()->protocol_name() == MXS_MARIADB_PROTOCOL_NAME)
        {
            const auto& conns = session->backend_connections();
            std::vector<BackendDCB*> incomplete_conns;

            std::unique_lock<std::mutex> lock(targets_lock);
            for (auto* conn : conns)
            {
                auto* maria_conn = static_cast<MariaDBBackendConnection*>(conn);
                uint64_t backend_thread_id = maria_conn->thread_id();
                if (backend_thread_id)
                {
                    // We know the thread ID so we can kill it.
                    auto srv = maria_conn->dcb()->server();
                    targets.insert(srv);
                    be_thread_ids[srv] = backend_thread_id;
                }
                else
                {
                    incomplete_conns.push_back(maria_conn->dcb());
                }
            }
            lock.unlock();

            for (auto dcb : incomplete_conns)
            {
                MXB_AT_DEBUG(MXB_WARNING(
                    "Forcefully closing incomplete connection to %s for session %lu.",
                    dcb->whoami().c_str(), session->id()));

                // DCB is not yet connected, send a hangup to forcibly close it
                session->close_reason = SESSION_CLOSE_KILLED;
                dcb->trigger_hangup_event();
            }
        }
    }
}

std::string ConnKillInfo::generate_kill_query(SERVER* target_server)
{
    auto it = be_thread_ids.find(target_server);
    mxb_assert(it != be_thread_ids.end());
    return mxb::string_printf("%s%lu", query_base.c_str(), it->second);
}

void UserKillInfo::generate_target_list(mxs::RoutingWorker* worker)
{
    const auto& sessions = worker->session_registry();
    for (auto it : sessions)
    {
        auto* session = it.second;
        if (strcasecmp(session->user().c_str(), user.c_str()) == 0
            && session->protocol()->protocol_name() == MXS_MARIADB_PROTOCOL_NAME)
        {
            const auto& conns = session->backend_connections();
            std::lock_guard<std::mutex> guard(targets_lock);
            for (auto* conn : conns)
            {
                targets.insert(conn->dcb()->server());
            }
        }
    }
}

std::string UserKillInfo::generate_kill_query(SERVER* target_server)
{
    return query_base;
}

MariaDBClientConnection::SSLState MariaDBClientConnection::ssl_authenticate_check_status()
{
    /**
     * We record the SSL status before and after ssl authentication. This allows
     * us to detect if the SSL handshake is immediately completed, which means more
     * data needs to be read from the socket.
     */
    bool health_before = (m_dcb->ssl_state() == DCB::SSLState::ESTABLISHED);
    int ssl_ret = ssl_authenticate_client();
    bool health_after = (m_dcb->ssl_state() == DCB::SSLState::ESTABLISHED);

    auto rval = SSLState::FAIL;
    if (ssl_ret != 0)
    {
        rval = (ssl_ret == SSL_ERROR_CLIENT_NOT_SSL) ? SSLState::NOT_CAPABLE : SSLState::FAIL;
    }
    else if (!health_after)
    {
        rval = SSLState::INCOMPLETE;
    }
    else if (!health_before && health_after)
    {
        rval = SSLState::INCOMPLETE;
        m_dcb->trigger_read_event();
    }
    else if (health_before && health_after)
    {
        rval = SSLState::COMPLETE;
    }
    return rval;
}

/**
 * Start or continue ssl handshake. If the listener requires SSL but the client is not SSL capable,
 * an error message is recorded and failure return given.
 *
 * @return 0 if ok, >0 if a problem - see return codes defined in ssl.h
 */
int MariaDBClientConnection::ssl_authenticate_client()
{
    auto dcb = m_dcb;

    const char* remote = m_dcb->remote().c_str();
    const char* service = m_session->service->name();

    /* Now we require an SSL connection */
    if (!m_session_data->ssl_capable())
    {
        /* Should be SSL, but client is not SSL capable. Cannot print the username, as client has not
         * sent that yet. */
        MXB_INFO("Client from '%s' attempted to connect to service '%s' without SSL when SSL was required.",
                 remote, service);
        return SSL_ERROR_CLIENT_NOT_SSL;
    }

    /* Now we know SSL is required and client is capable */
    if (m_dcb->ssl_state() != DCB::SSLState::ESTABLISHED)
    {
        /**
         * This will often not complete because further reading (or possibly writing) of SSL-related
         * information is needed. DCB::process_events() calls DCB::ssl_handshake() on EPOLLIN-event
         * while the SSL state is SSL_HANDSHAKE_REQUIRED.
         */
        int return_code = dcb->ssl_start_accept();
        if (return_code < 0)
        {
            MXB_INFO("Client from '%s' failed to connect to service '%s' with SSL.", remote, service);
            return SSL_ERROR_ACCEPT_FAILED;
        }
        else if (mxb_log_should_log(LOG_INFO))
        {
            if (return_code == 1)
            {
                MXB_INFO("Client from '%s' connected to service '%s' with SSL.", remote, service);
            }
            else
            {
                MXB_INFO("Client from '%s' is in progress of connecting to service '%s' with SSL.",
                         remote, service);
            }
        }
    }
    return SSL_AUTH_CHECKS_OK;
}

/**
 * Send the server handshake packet to the client.
 *
 * @return True on success
 */
bool MariaDBClientConnection::send_server_handshake()
{
    auto service = m_session->service;
    packet_parser::ByteVec payload;
    // The exact size depends on a few factors, reserve enough to avoid reallocations in most cases.
    payload.reserve(130);

    // Contents as in https://mariadb.com/kb/en/connection/#initial-handshake-packet
    payload.push_back((uint8_t)GW_MYSQL_PROTOCOL_VERSION);
    payload.push_back(get_version_string(service));

    // The length of the following fields all the way until plugin name is 44.
    const int id_to_plugin_bytes = 44;
    auto orig_size = payload.size();
    payload.resize(orig_size + id_to_plugin_bytes);
    auto ptr = payload.data() + orig_size;

    // Use the session id as the server thread id. Only the low 32bits are sent in the handshake.
    mariadb::set_byte4(ptr, m_session->id());
    ptr += 4;

    /* gen_random_bytes() generates random bytes (0-255). This is ok as scramble for most clients
     * (e.g. mariadb) but not for mysql-connector-java. To be on the safe side, ensure every byte
     * is a non-whitespace character. To do the rescaling of values without noticeable bias, generate
     * double the required bytes.
     */
    uint8_t random_bytes[2 * MYSQL_SCRAMBLE_LEN];
    mxb::Worker::gen_random_bytes(random_bytes, sizeof(random_bytes));
    auto* scramble_storage = m_session_data->scramble;
    for (size_t i = 0; i < MYSQL_SCRAMBLE_LEN; i++)
    {
        auto src = &random_bytes[2 * i];
        auto val16 = *(reinterpret_cast<uint16_t*>(src));
        scramble_storage[i] = '!' + (val16 % (('~' + 1) - '!'));
    }

    // Write scramble part 1.
    ptr = mariadb::copy_bytes(ptr, scramble_storage, 8);

    // Filler byte.
    *ptr++ = 0;

    auto [cap_types, min_version, caps] = get_supported_cap_types(service);

    if (cap_types == CapTypes::MARIADB)
    {
        // A MariaDB 10.2 server or later omits the CLIENT_MYSQL capability. This signals that it supports
        // extended capabilities.
        caps &= ~GW_MYSQL_CAPABILITIES_CLIENT_MYSQL;
        caps |= MXS_EXTRA_CAPS_SERVER64;

        if (min_version < 100600)
        {
            // The metadata caching was added in 10.6 and should only be enabled if all nodes support it.
            caps &= ~(MXS_MARIA_CAP_CACHE_METADATA << 32);

            if (min_version < 100500)
            {
                caps &= ~(MXS_MARIA_CAP_EXTENDED_TYPES << 32);
            }
        }
    }

    if (m_session->capabilities() & RCAP_TYPE_OLD_PROTOCOL)
    {
        // Some module requires that only the base protocol is used, most likely due to the fact
        // that it processes the contents of the resultset.
        const uint64_t extensions = MXS_MARIA_CAP_CACHE_METADATA | MXS_MARIA_CAP_EXTENDED_TYPES;
        caps &= ~((extensions << 32) | GW_MYSQL_CAPABILITIES_DEPRECATE_EOF);
        mxb_assert((caps & MXS_EXTRA_CAPS_SERVER64) == (MXS_MARIA_CAP_STMT_BULK_OPERATIONS << 32)
                   || cap_types != CapTypes::MARIADB);
        mxb_assert((caps & GW_MYSQL_CAPABILITIES_DEPRECATE_EOF) == 0);
    }

    if (cap_types == CapTypes::XPAND || min_version < 80000 || (min_version > 100000 && min_version < 100208))
    {
        // The DEPRECATE_EOF and session tracking were added in MySQL 5.7, anything older than that shouldn't
        // advertise them. This includes XPand: it doesn't support SESSION_TRACK or DEPRECATE_EOF as it's
        // MySQL 5.1 compatible on the protocol layer. Additionally, MySQL 5.7 has a broken query cache
        // implementation where it sends non-DEPRECATE_EOF results even when a client requested results in the
        // DEPRECATE_EOF format. The same query cache bug was present in MariaDB but was fixed in 10.2.8
        // (MDEV-13300).
        caps &= ~(GW_MYSQL_CAPABILITIES_SESSION_TRACK | GW_MYSQL_CAPABILITIES_DEPRECATE_EOF);
    }

    if (require_ssl())
    {
        caps |= GW_MYSQL_CAPABILITIES_SSL;
    }

    m_session_data->client_caps.advertised_capabilities = caps;

    // 8 bytes of capabilities, sent in three parts.
    // Convert to little endian, write 2 bytes.
    uint8_t caps_le[8];
    mariadb::set_byte8(caps_le, caps);
    ptr = mariadb::copy_bytes(ptr, caps_le, 2);

    // Character set.
    uint8_t charset = service->charset();
    if (charset == 0)
    {
        charset = 8;        // Charset 8 is latin1, the server default.
    }
    *ptr++ = charset;

    uint16_t status_flags = 2;      // autocommit enabled
    mariadb::set_byte2(ptr, status_flags);
    ptr += 2;

    // More capabilities.
    ptr = mariadb::copy_bytes(ptr, caps_le + 2, 2);

    *ptr++ = MYSQL_SCRAMBLE_LEN + 1;    // Plugin data total length, contains 1 filler.

    // 6 bytes filler
    ptr = mariadb::set_bytes(ptr, 0, 6);

    // Capabilities part 3 or 4 filler bytes.
    ptr = cap_types == CapTypes::MARIADB ?
        mariadb::copy_bytes(ptr, caps_le + 4, 4) :
        mariadb::set_bytes(ptr, 0, 4);

    // Scramble part 2.
    ptr = mariadb::copy_bytes(ptr, scramble_storage + 8, 12);

    // filler
    *ptr++ = 0;

    mxb_assert(ptr - (payload.data() + orig_size) == id_to_plugin_bytes);
    // Add plugin name.
    payload.push_back(base_plugin);

    bool rval = false;
    // Allocate buffer and send.
    auto pl_size = payload.size();
    GWBUF buf(MYSQL_HEADER_LEN + pl_size);
    ptr = buf.data();
    ptr = mariadb::write_header(ptr, pl_size, 0);
    memcpy(ptr, payload.data(), pl_size);
    return write(std::move(buf));
}

/**
 * Start or continue authenticating the client.
 *
 * @return Instruction for upper level state machine
 */
MariaDBClientConnection::StateMachineRes
MariaDBClientConnection::process_authentication(AuthType auth_type)
{
    auto rval = StateMachineRes::IN_PROGRESS;
    bool state_machine_continue = true;
    auto& auth_data = (auth_type == AuthType::NORMAL_AUTH) ? *m_session_data->auth_data :
        *m_change_user.auth_data;
    const auto& user_entry_type = auth_data.user_entry.type;

    auto bad_user_account = [this, &state_machine_continue, &user_entry_type]() {
        // Something is wrong with the entry. Authentication will likely fail.
        mxb_assert(user_entry_type != UserEntryType::NEED_NAMEINFO);
        if (user_account_cache()->can_update_immediately())
        {
            // User data may be outdated, send update message through the service.
            // The current session will stall until userdata has been updated.
            m_session->service->request_user_account_update();
            m_session->service->mark_for_wakeup(this);
            m_auth_state = AuthState::TRY_AGAIN;
            state_machine_continue = false;
        }
        else
        {
            MXB_WARNING(MariaDBUserManager::RECENTLY_UPDATED_FMT,
                        m_session_data->user_and_host().c_str());
            // If plugin exists, start exchange. Authentication will surely fail.
            m_auth_state = (user_entry_type == UserEntryType::PLUGIN_IS_NOT_LOADED) ?
                AuthState::NO_PLUGIN : AuthState::START_EXCHANGE;
        }
    };

    auto data_during_rdns = [this]() {
        MXB_ERROR("Client %s sent data when waiting for reverse name lookup. Closing session.",
                  m_session_data->user_and_host().c_str());
        send_misc_error("Unexpected client event");
        m_auth_state = AuthState::FAIL;
    };

    while (state_machine_continue)
    {
        switch (m_auth_state)
        {
        case AuthState::FIND_ENTRY:
            {
                if (m_session_data->user_search_settings.listener.passthrough_auth)
                {
                    set_passthrough_account_entry(auth_data);
                }
                else
                {
                    update_user_account_entry(auth_data);
                }

                if (user_entry_type == UserEntryType::USER_ACCOUNT_OK)
                {
                    m_auth_state = AuthState::START_EXCHANGE;
                }
                else if (user_entry_type == UserEntryType::NEED_NAMEINFO)
                {
                    schedule_reverse_name_lookup();
                    m_auth_state = AuthState::FIND_ENTRY_RDNS;
                    state_machine_continue = false;
                }
                else
                {
                    bad_user_account();
                }
            }
            break;

        case AuthState::FIND_ENTRY_RDNS:
            if (m_session_data->host.has_value())
            {
                update_user_account_entry(auth_data);
                if (user_entry_type == UserEntryType::USER_ACCOUNT_OK)
                {
                    m_auth_state = AuthState::START_EXCHANGE;
                }
                else
                {
                    bad_user_account();
                }
            }
            else
            {
                data_during_rdns();
            }
            break;

        case AuthState::TRY_AGAIN:
            {
                // Waiting for user account update.
                if (m_user_update_wakeup)
                {
                    // Only recheck user if the user account data has actually changed since the previous
                    // attempt.
                    if (user_account_cache()->version() > m_previous_userdb_version)
                    {
                        update_user_account_entry(auth_data);
                    }

                    if (user_entry_type == UserEntryType::USER_ACCOUNT_OK)
                    {
                        MXB_DEBUG("Found user account entry for %s after updating user account data.",
                                  m_session_data->user_and_host().c_str());
                        m_auth_state = AuthState::START_EXCHANGE;
                    }
                    else if (user_entry_type == UserEntryType::NEED_NAMEINFO)
                    {
                        schedule_reverse_name_lookup();
                        m_auth_state = AuthState::TRY_AGAIN_RDNS;
                        state_machine_continue = false;
                    }
                    else
                    {
                        m_auth_state = (user_entry_type == UserEntryType::PLUGIN_IS_NOT_LOADED) ?
                            AuthState::NO_PLUGIN : AuthState::START_EXCHANGE;
                    }
                }
                else
                {
                    // Should not get client data (or read events) before users have actually been updated.
                    // This can happen if client hangs up while MaxScale is waiting for the update.
                    MXB_ERROR("Client %s sent data when waiting for user account update. Closing session.",
                              m_session_data->user_and_host().c_str());
                    send_misc_error("Unexpected client event");
                    // Unmark because auth state is modified.
                    m_session->service->unmark_for_wakeup(this);
                    m_auth_state = AuthState::FAIL;
                }
            }
            break;

        case AuthState::TRY_AGAIN_RDNS:
            if (m_session_data->host.has_value())
            {
                update_user_account_entry(auth_data);
                if (user_entry_type == UserEntryType::USER_ACCOUNT_OK)
                {
                    MXB_DEBUG("Found user account entry for %s after updating user account data.",
                              m_session_data->user_and_host().c_str());
                    m_auth_state = AuthState::START_EXCHANGE;
                }
                else
                {
                    m_auth_state = (user_entry_type == UserEntryType::PLUGIN_IS_NOT_LOADED) ?
                        AuthState::NO_PLUGIN : AuthState::START_EXCHANGE;
                }
            }
            else
            {
                data_during_rdns();
            }
            break;

        case AuthState::NO_PLUGIN:
            send_authentication_error(AuthErrorType::NO_PLUGIN);
            m_auth_state = AuthState::FAIL;
            break;

        case AuthState::START_EXCHANGE:
            state_machine_continue = perform_auth_exchange(GWBUF(), auth_data);
            break;

        case AuthState::CONTINUE_EXCHANGE:
            state_machine_continue = read_and_auth_exchange(auth_data);
            break;

        case AuthState::CHECK_TOKEN:
            perform_check_token(auth_type);
            break;

        case AuthState::START_SESSION:
            {
                // Authentication success, initialize session. Backend authenticator must be set before
                // connecting to backends.
                bool session_started = false;
                if (m_session_data->user_search_settings.listener.passthrough_auth)
                {
                    assign_backend_authenticator(auth_data);
                    if (m_session->start())
                    {
                        m_auth_state = AuthState::WAIT_FOR_BACKEND;
                        m_session_data->passthrough_be_auth_cb = [this](GWBUF&& auth_reply) {
                            deliver_backend_auth_result(std::move(auth_reply));
                        };
                        session_started = true;
                        state_machine_continue = false;
                    }
                }
                else
                {
                    m_session_data->current_db = auth_data.default_db;
                    m_session_data->role = auth_data.user_entry.entry.default_role;
                    assign_backend_authenticator(auth_data);
                    if (m_session->start())
                    {
                        mxb_assert(m_session->state() != MXS_SESSION::State::CREATED);
                        m_auth_state = AuthState::COMPLETE;
                        session_started = true;
                    }
                }

                if (!session_started)
                {
                    // Send internal error, as in this case the client has done nothing wrong.
                    send_mysql_err_packet(1815, "HY000", "Internal error: Session creation failed");
                    MXB_ERROR("Failed to create session for %s.", m_session_data->user_and_host().c_str());
                    m_auth_state = AuthState::FAIL;
                }
            }
            break;

        case AuthState::CHANGE_USER_OK:
            {
                // Reauthentication to MaxScale succeeded, but the query still needs to be successfully
                // routed.
                rval = complete_change_user_p1() ? StateMachineRes::DONE : StateMachineRes::ERROR;
                state_machine_continue = false;
                break;
            }

        case AuthState::WAIT_FOR_BACKEND:
            switch (m_pt_be_auth_res)
            {
            case PtAuthResult::OK:
                m_session_data->current_db = auth_data.default_db;
                // Don't set current role as we don't know it.
                m_auth_state = AuthState::COMPLETE;
                break;

            case PtAuthResult::ERROR:
                // Error was already sent to client, let state machine continue to session closure.
                m_auth_state = AuthState::FAIL;
                break;

            case PtAuthResult::NONE:
                // Should not get client data (or read events) before authentication to backend is complete.
                MXB_ERROR("Client %s sent data when waiting for passthrough authentication. Closing session.",
                          m_session_data->user_and_host().c_str());
                send_misc_error("Unexpected client event");
                m_auth_state = AuthState::FAIL;
                break;
            }
            break;

        case AuthState::COMPLETE:
            m_sql_mode = m_session->listener_data()->m_default_sql_mode;
            write_ok_packet(m_next_sequence);
            if (!m_dcb->readq_empty())
            {
                // The user has already sent more data, process it
                m_dcb->trigger_read_event();
            }
            state_machine_continue = false;
            rval = StateMachineRes::DONE;
            break;

        case AuthState::FAIL:
            // An error message should have already been sent.
            state_machine_continue = false;
            if (auth_type == AuthType::NORMAL_AUTH)
            {
                rval = StateMachineRes::ERROR;
            }
            else
            {
                // com_change_user failed, but the session may yet continue.
                cancel_change_user_p1();
                rval = StateMachineRes::DONE;
            }

            break;
        }
    }
    return rval;
}

void MariaDBClientConnection::update_user_account_entry(mariadb::AuthenticationData& auth_data)
{
    const auto mses = m_session_data;
    auto* users = user_account_cache();
    auto search_res = users->find_user(auth_data.user, auth_data.default_db, mses);
    // NEED_NAMEINFO is a special case and skips other checks.
    if (search_res.type != UserEntryType::NEED_NAMEINFO)
    {
        m_previous_userdb_version = users->version();   // Can use this to skip user entry check after update.

        mariadb::AuthenticatorModule* selected_module = find_auth_module(search_res.entry.plugin);
        if (selected_module)
        {
            // Correct plugin is loaded, generate session-specific data.
            auth_data.client_auth_module = selected_module;
            // If changing user, this overrides the old client authenticator. Not an issue, as the client auth
            // is only used during authentication.
            m_authenticator = selected_module->create_client_authenticator(*this);
        }
        else
        {
            // Authentication cannot continue in this case. Should be rare, though.
            search_res.type = UserEntryType::PLUGIN_IS_NOT_LOADED;
            MXB_INFO("User entry '%s'@'%s' uses unrecognized authenticator plugin '%s'. "
                     "Cannot authenticate user.",
                     search_res.entry.username.c_str(), search_res.entry.host_pattern.c_str(),
                     search_res.entry.plugin.c_str());
        }
    }
    auth_data.user_entry = move(search_res);
}

void MariaDBClientConnection::set_passthrough_account_entry(mariadb::AuthenticationData& auth_data)
{
    auto& auth_modules = m_session->listener_data()->m_authenticators;
    mxb_assert(auth_modules.size() == 1);
    auto* selected_module = static_cast<mariadb::AuthenticatorModule*>(auth_modules[0].get());
    auth_data.client_auth_module = selected_module;

    // The authenticator class should not change, but this may reset some fields.
    m_authenticator = selected_module->create_client_authenticator(*this);

    // Imagine that the user account was found.
    auth_data.user_entry.type = UserEntryType::USER_ACCOUNT_OK;
    // Leave the other fields of 'user_entry' unset, as they are only accessed when doing normal
    // authentication.
}

/**
 * Handle relevant variables.
 *
 * @param buffer  Buffer, assumed to contain a statement.
 * @return Empty if successful, otherwise the error message.
 */
string MariaDBClientConnection::handle_variables(GWBUF& buffer)
{
    string message;
    SetParser set_parser;
    SetParser::Result result;

    switch (set_parser.check(mariadb::get_sql(buffer), &result))
    {
    case SetParser::ERROR:
        // In practice only OOM.
        break;

    case SetParser::IS_SET_SQL_MODE:
        {
            SqlModeParser sql_mode_parser;

            const SetParser::Result::Items& values = result.values();

            for (const auto& value : values)
            {
                switch (sql_mode_parser.get_sql_mode(value.first, value.second))
                {
                case SqlModeParser::ORACLE:
                    m_session_data->set_autocommit(false);
                    m_sql_mode = mxs::Parser::SqlMode::ORACLE;
                    break;

                case SqlModeParser::DEFAULT:
                    m_session_data->set_autocommit(true);
                    m_sql_mode = mxs::Parser::SqlMode::DEFAULT;
                    break;

                case SqlModeParser::SOMETHING:
                    break;

                default:
                    mxb_assert(!true);
                }
            }
        }
        break;

    case SetParser::IS_SET_MAXSCALE:
        {
            const SetParser::Result::Items& variables = result.variables();
            const SetParser::Result::Items& values = result.values();

            auto i = variables.begin();
            auto j = values.begin();

            while (message.empty() && (i != variables.end()))
            {
                const SetParser::Result::Item& variable = *i;
                const SetParser::Result::Item& value = *j;
                message = m_session->set_variable_value(variable.first, variable.second,
                                                        value.first, value.second);
                ++i;
                ++j;
            }
        }
        break;

    case SetParser::NOT_RELEVANT:
        break;

    default:
        mxb_assert(!true);
    }

    return message;
}

void MariaDBClientConnection::handle_query_kill(const SpecialQueryDesc& kill_contents)
{
    auto kt = kill_contents.kill_options;
    auto& user = kill_contents.target;
    // TODO: handle "query id" somehow
    if ((kt & KT_QUERY_ID) == 0)
    {
        if (kill_contents.kill_id > 0)
        {
            execute_kill_connection(kill_contents.kill_id, (kill_type_t)kt);
        }
        else if (!user.empty())
        {
            execute_kill_user(user.c_str(), (kill_type_t)kt);
        }
        else
        {
            write_ok_packet(1);
        }
    }
}

MariaDBClientConnection::SpecialQueryDesc
MariaDBClientConnection::parse_kill_query_elems(const char* sql)
{
    const string connection = "connection";
    const string query = "query";
    const string hard = "hard";
    const string soft = "soft";

    auto& regex = this_unit.special_queries_regex;

    auto option = mxb::tolower(regex.substring_by_name(sql, "koption"));
    auto type = mxb::tolower(regex.substring_by_name(sql, "ktype"));
    auto target = mxb::tolower(regex.substring_by_name(sql, "ktarget"));

    SpecialQueryDesc rval;
    rval.type = SpecialQueryDesc::Type::KILL;

    // Option is either "hard", "soft", or empty.
    if (option == hard)
    {
        rval.kill_options |= KT_HARD;
    }
    else if (option == soft)
    {
        rval.kill_options |= KT_SOFT;
    }
    else
    {
        mxb_assert(option.empty());
    }

    // Type is either "connection", "query", "query\s+id" or empty.
    if (type == connection)
    {
        rval.kill_options |= KT_CONNECTION;
    }
    else if (type == query)
    {
        rval.kill_options |= KT_QUERY;
    }
    else if (!type.empty())
    {
        mxb_assert(type.find(query) == 0);
        rval.kill_options |= KT_QUERY_ID;
    }

    // target is either a query/thread id or "user\s+<username>"
    if (isdigit(target[0]))
    {
        mxb::get_uint64(target.c_str(), &rval.kill_id);
    }
    else
    {
        auto words = mxb::strtok(target, " ");
        rval.target = words[1];
    }
    return rval;
}

void MariaDBClientConnection::handle_use_database(GWBUF& read_buffer)
{
    auto databases = parser()->get_database_names(read_buffer);
    if (!databases.empty())
    {
        start_change_db(string(databases[0]));
    }
}

bool MariaDBClientConnection::should_inspect_query(GWBUF& buffer) const
{
    bool rval = true;

    if (parser()->parse(buffer, mxs::Parser::COLLECT_ALL) == mxs::Parser::Result::PARSED)
    {
        auto op = parser()->get_operation(buffer);

        if (op != mxs::sql::OP_KILL && op != mxs::sql::OP_SET && op != mxs::sql::OP_CHANGE_DB)
        {
            rval = false;
        }
    }

    return rval;
}

/**
 * Some SQL commands/queries need to be detected and handled by the protocol
 * and MaxScale instead of being routed forward as is.
 *
 * @param buffer Query buffer
 * @return see @c spec_com_res_t
 */
MariaDBClientConnection::SpecialCmdRes
MariaDBClientConnection::process_special_queries(GWBUF& buffer)
{
    auto rval = SpecialCmdRes::CONTINUE;

    auto packet_len = buffer.length();
    /* The packet must be at least HEADER + cmd + 5 (USE d) chars in length. Also, if the packet is rather
     * long, assume that it is not a tracked query. This assumption allows avoiding the
     * make_contiquous-call
     * on e.g. big inserts. The long packets can only contain one of the tracked queries by having lots of
     * comments. */
    const size_t min_len = MYSQL_HEADER_LEN + 1 + 5;
    const size_t max_len = 10000;

    if (packet_len >= min_len && packet_len <= max_len)
    {
        const char* sql = nullptr;
        int len = 0;
        bool is_special = false;

        std::string_view sv = mariadb::get_sql(buffer);

        if (!sv.empty())
        {
            sql = sv.data();
            len = sv.length();
            auto pEnd = sql + len;
            is_special = detect_special_query(&sql, pEnd);
            len = pEnd - sql;
        }

        if (is_special)
        {
            auto fields = parse_special_query(sql, len);
            switch (fields.type)
            {
            case SpecialQueryDesc::Type::NONE:
                break;

            case SpecialQueryDesc::Type::KILL:
                handle_query_kill(fields);
                // The kill-query is not routed to backends, as the id:s would be wrong.
                rval = SpecialCmdRes::END;
                break;

            case SpecialQueryDesc::Type::USE_DB:
                handle_use_database(buffer);
                break;

            case SpecialQueryDesc::Type::SET_ROLE:
                start_change_role(move(fields.target));
                break;
            }
        }
    }

    return rval;
}

bool MariaDBClientConnection::record_for_history(GWBUF& buffer, uint8_t cmd)
{
    bool should_record = false;

    // Update the routing information. This must be done even if the command isn't added to the history.
    const auto& info = m_qc.update_route_info(buffer);

    switch (cmd)
    {
    case MXS_COM_QUIT:      // The client connection is about to be closed
    case MXS_COM_PING:      // Doesn't change the state so it doesn't need to be stored
    case MXS_COM_STMT_RESET:// Resets the prepared statement state, not needed by new connections
        break;

    case MXS_COM_STMT_EXECUTE:
        {
            uint32_t id = mxs_mysql_extract_ps_id(buffer);
            uint16_t params = m_qc.get_param_count(id);

            if (params > 0)
            {
                size_t types_offset = MYSQL_HEADER_LEN + 1 + 4 + 1 + 4 + ((params + 7) / 8);
                uint8_t* ptr = buffer.data() + types_offset;

                if (*ptr)
                {
                    ++ptr;

                    // Store the metadata, two bytes per parameter, for later use. The backends will need if
                    // it they have to reconnect and re-executed the prepared statements.
                    m_session_data->exec_metadata[id].assign(ptr, ptr + (params * 2));
                }
            }
        }
        break;

    case MXS_COM_STMT_CLOSE:
        {
            // Instead of handling COM_STMT_CLOSE like a normal command, we can exclude it from the history as
            // well as remove the original COM_STMT_PREPARE that it refers to. This simplifies the history
            // replay as all stored commands generate a response and none of them refer to any previous
            // commands. This means that the history can be executed in a single batch without waiting for any
            // responses.
            uint32_t id = mxs_mysql_extract_ps_id(buffer);

            if (m_session_data->history().erase(id))
            {
                mxb_assert(id);
                m_session_data->exec_metadata.erase(id);
            }
        }
        break;

    case MXS_COM_CHANGE_USER:
        // COM_CHANGE_USER resets the whole connection. Any new connections will already be using the new
        // credentials which means we can safely reset the history here.
        m_session_data->history().clear();
        m_session_data->exec_metadata.clear();
        break;

    case MXS_COM_STMT_PREPARE:
        should_record = true;
        break;

    default:
        should_record = m_qc.target_is_all(info.target());
        break;
    }

    if (should_record)
    {
        buffer.set_id(m_next_id);
        // Keep a copy for the session command history. The buffer originates from the dcb, so deep clone
        // it to minimize memory use. Also saves an allocation when reading the server reply.
        m_pending_cmd = buffer.deep_clone();
        should_record = true;

        if (++m_next_id == MAX_SESCMD_ID)
        {
            m_next_id = 1;
        }
    }

    m_qc.commit_route_info_update(buffer);

    return should_record;
}

/**
 * Route an SQL protocol packet. If the original client packet is less than 16MB, buffer should
 * contain the complete packet. If the client packet is large (split into multiple protocol packets),
 * only one protocol packet should be routed at a time.
 * TODO: what happens with parsing in this case? Likely it fails.
 *
 * @param buffer Buffer to route
 * @return True on success
 */
bool MariaDBClientConnection::route_statement(GWBUF&& buffer)
{
    bool recording = false;
    uint8_t cmd = mariadb::get_command(buffer);

    if (m_session->capabilities() & RCAP_TYPE_SESCMD_HISTORY)
    {
        recording = record_for_history(buffer, cmd);
    }
    else if (cmd == MXS_COM_STMT_PREPARE)
    {
        buffer.set_id(m_next_id);

        if (++m_next_id == MAX_SESCMD_ID)
        {
            m_next_id = 1;
        }
    }

    // Must be done whether or not there were any changes, as the query classifier
    // is thread and not session specific.
    parser()->set_sql_mode(m_sql_mode);
    // The query classifier classifies according to the service's server that has
    // the smallest version number.
    parser()->set_server_version(m_version);

    auto service = m_session->service;
    auto capabilities = m_session->capabilities();
    auto& tracker = m_session_data->trx_tracker();

    if (rcap_type_required(capabilities, RCAP_TYPE_SESCMD_HISTORY))
    {
        // The transaction state can be copied from m_qc where it was already tracked by the session
        // command history code.
        tracker = m_qc.current_route_info().trx();
    }
    else if (rcap_type_required(capabilities, RCAP_TYPE_QUERY_CLASSIFICATION))
    {
        tracker.track_transaction_state(buffer, MariaDBParser::get());
    }
    else
    {
        constexpr auto CUSTOM = mxs::Parser::ParseTrxUsing::CUSTOM;
        tracker.track_transaction_state<CUSTOM>(buffer, MariaDBParser::get());
    }

    // TODO: The response count and state is currently modified before we route the query to allow routers to
    // call clientReply inside routeQuery. This should be changed so that routers don't directly call
    // clientReply and instead it to be delivered in a separate event.

    bool expecting_response = mxs_mysql_command_will_respond(cmd);

    if (expecting_response)
    {
        m_session->retain_statement(buffer);
    }

    if (recording)
    {
        mxb_assert(expecting_response);
        m_routing_state = RoutingState::RECORD_HISTORY;
    }

    bool ok = m_downstream->routeQuery(move(buffer));

    if (ok && expecting_response)
    {
        ++m_num_responses;
    }

    return ok;
}

void MariaDBClientConnection::finish_recording_history(const mxs::Reply& reply)
{
    if (reply.is_complete())
    {
        MXB_INFO("Added %s to history with ID %u: %s (result: %s)",
                 mariadb::cmd_to_string(m_pending_cmd[4]), m_pending_cmd.id(),
                 maxbase::show_some(string(mariadb::get_sql(m_pending_cmd)), 200).c_str(),
                 reply.is_ok() ? "OK" : reply.error().message().c_str());

        // Check the early responses to this command that arrived and were discarded before the accepted
        // response that ended up here was received. Doing this with lcall() allows the command ID and the
        // result to be stored inside it which removes the need to permanently store the latest command in
        // MariaDBClientConnection as a member variable.
        m_session->worker()->lcall([this, id = m_pending_cmd.id(), ok = reply.is_ok()](){
            if (m_session->is_alive())
            {
                m_session_data->history().check_early_responses(id, ok);
            }
        });

        m_routing_state = RoutingState::PACKET_START;
        m_session_data->history().add(std::move(m_pending_cmd), reply.is_ok());
        m_pending_cmd.clear();

        // There's possibly another packet ready to be read in either the DCB's readq or in the socket. This
        // happens for example when direct execution of prepared statements is done where the COM_STMT_PREPARE
        // and COM_STMT_EXECUTE are sent as one batch.
        m_dcb->trigger_read_event();
    }
}

/**
 * @brief Client read event, process data, client already authenticated
 *
 * First do some checks and get the router capabilities.  If the router
 * wants to process each individual statement, then the data must be split
 * into individual SQL statements. Any data that is left over is held in the
 * DCB read queue.
 *
 * Finally, the general client data processing function is called.
 *
 * @return True if session should continue, false if client connection should be closed
 */
MariaDBClientConnection::StateMachineRes MariaDBClientConnection::process_normal_read()
{
    auto session_state_value = m_session->state();
    if (session_state_value != MXS_SESSION::State::STARTED)
    {
        if (session_state_value != MXS_SESSION::State::STOPPING)
        {
            MXB_ERROR("Session received a query in incorrect state: %s",
                      session_state_to_string(session_state_value));
        }
        return StateMachineRes::ERROR;
    }

    if (m_routing_state == RoutingState::CHANGING_STATE
        || m_routing_state == RoutingState::RECORD_HISTORY)
    {
        // We're still waiting for a response from the backend, read more data once we get it.
        return StateMachineRes::IN_PROGRESS;
    }

    auto [read_ok, buffer] = read_protocol_packet();
    if (buffer.empty())
    {
        // Either an error or an incomplete packet.
        return read_ok ? StateMachineRes::IN_PROGRESS : StateMachineRes::ERROR;
    }

    bool routed = false;

    switch (m_routing_state)
    {
    case RoutingState::PACKET_START:
        if (buffer.length() > MYSQL_HEADER_LEN)
        {
            routed = process_normal_packet(move(buffer));
        }
        else
        {
            // Unexpected, client should not be sending empty (header-only) packets in this case.
            MXB_ERROR("Client %s sent empty packet when a normal packet was expected.",
                      m_session->user_and_host().c_str());
        }
        break;

    case RoutingState::LARGE_PACKET:
        {
            // No command bytes, just continue routing large packet.
            bool is_large = large_query_continues(buffer);
            routed = m_downstream->routeQuery(move(buffer)) != 0;

            if (!is_large)
            {
                // Large packet routing completed.
                m_routing_state = RoutingState::PACKET_START;
            }
        }
        break;

    case RoutingState::LARGE_HISTORY_PACKET:
        {
            // A continuation of a recoded command, append it to the current command and route it forward
            bool is_large = large_query_continues(buffer);
            m_pending_cmd.append(buffer);
            routed = m_downstream->routeQuery(move(buffer)) != 0;

            if (!is_large)
            {
                // Large packet routing completed.
                m_routing_state = RoutingState::RECORD_HISTORY;
                mxb_assert(m_pending_cmd.length() > MYSQL_PACKET_LENGTH_MAX + MYSQL_HEADER_LEN);
            }
        }
        break;

    case RoutingState::LOAD_DATA:
        {
            // Local-infile routing continues until client sends an empty packet. Again, tracked by backend
            // but this time on the downstream side.
            routed = m_downstream->routeQuery(move(buffer)) != 0;
        }
        break;

    case RoutingState::CHANGING_STATE:
    case RoutingState::RECORD_HISTORY:
        mxb_assert_message(!true, "We should never end up here");
        break;
    }

    auto rval = StateMachineRes::IN_PROGRESS;
    if (!routed)
    {
        /** Routing failed, close the client connection */
        m_session->close_reason = SESSION_CLOSE_ROUTING_FAILED;
        rval = StateMachineRes::ERROR;
        MXB_ERROR("Routing the query failed. Session will be closed.");
    }
    else if (m_command == MXS_COM_QUIT)
    {
        /** Close router session which causes closing of backends */
        mxb_assert_message(m_session->normal_quit(), "Session should be quitting normally");
        m_state = State::QUIT;
        rval = StateMachineRes::DONE;
    }

    return rval;
}

void MariaDBClientConnection::ready_for_reading(DCB* event_dcb)
{
    mxb_assert(m_dcb == event_dcb);     // The protocol should only handle its own events.

    bool state_machine_continue = true;
    while (state_machine_continue)
    {
        switch (m_state)
        {
        case State::HANDSHAKING:
            /**
             * After a listener has accepted a new connection, a standard MySQL handshake is
             * sent to the client. The first time this function is called from the poll loop,
             * the client reply to the handshake should be available.
             */
            {
                auto ret = process_handshake();
                switch (ret)
                {
                case StateMachineRes::IN_PROGRESS:
                    state_machine_continue = false;     // need more data
                    break;

                case StateMachineRes::DONE:
                    m_state = State::AUTHENTICATING;        // continue directly to next state
                    break;

                case StateMachineRes::ERROR:
                    m_state = State::FAILED;
                    break;
                }
            }
            break;

        case State::AUTHENTICATING:
        case State::CHANGING_USER:
            {
                auto auth_type = (m_state == State::CHANGING_USER) ? AuthType::CHANGE_USER :
                    AuthType::NORMAL_AUTH;
                auto ret = process_authentication(auth_type);
                switch (ret)
                {
                case StateMachineRes::IN_PROGRESS:
                    state_machine_continue = false;     // need more data
                    break;

                case StateMachineRes::DONE:
                    if (auth_type == AuthType::NORMAL_AUTH)
                    {
                        // Allow pooling for fresh sessions. This allows pooling in situations where
                        // the client/connector does not send any queries at start and session stays idle.
                        m_session->set_can_pool_backends(true);
                    }
                    m_state = State::READY;
                    break;

                case StateMachineRes::ERROR:
                    m_state = State::FAILED;
                    break;
                }
            }
            break;

        case State::READY:
            {
                auto ret = process_normal_read();
                switch (ret)
                {
                case StateMachineRes::IN_PROGRESS:
                    state_machine_continue = false;
                    break;

                case StateMachineRes::DONE:
                    // In this case, next m_state was written by 'process_normal_read'.
                    break;

                case StateMachineRes::ERROR:
                    m_state = State::FAILED;
                    break;
                }
            }
            break;

        case State::QUIT:
        case State::FAILED:
            state_machine_continue = false;
            break;
        }
    }

    if (m_state == State::FAILED || m_state == State::QUIT)
    {
        m_session->kill();
    }
}

bool MariaDBClientConnection::write(GWBUF&& buffer)
{
    return m_dcb->writeq_append(move(buffer));
}

void MariaDBClientConnection::error(DCB* event_dcb, const char* error)
{
    mxb_assert(m_dcb == event_dcb);

    if (!m_session->normal_quit())
    {
        if (session_get_dump_statements() == SESSION_DUMP_STATEMENTS_ON_ERROR)
        {
            m_session->dump_statements();
        }

        if (session_get_session_trace())
        {
            m_session->dump_session_log();
        }

        // The client did not send a COM_QUIT packet
        std::string errmsg {"Connection killed by MaxScale"};
        std::string extra {session_get_close_reason(m_session)};

        if (!extra.empty())
        {
            errmsg += ": " + extra;
        }

        send_mysql_err_packet(1927, "08S01", errmsg.c_str());
    }

    // We simply close the session, this will propagate the closure to any
    // backend descriptors and perform the session cleanup.
    m_session->kill();
}

bool MariaDBClientConnection::init_connection()
{
    return send_server_handshake();
}

void MariaDBClientConnection::finish_connection()
{
    // If this connection is waiting for userdata, remove the entry.
    if (m_auth_state == AuthState::TRY_AGAIN)
    {
        m_session->service->unmark_for_wakeup(this);
    }

    if (m_session->capabilities() & RCAP_TYPE_SESCMD_HISTORY)
    {
        m_session->service->track_history_length(m_session_data->history().size());
    }
}

int32_t MariaDBClientConnection::connlimit(int limit)
{
    return write(mariadb::create_error_packet(0, 1040, "08004", "Too many connections"));
}

MariaDBClientConnection::MariaDBClientConnection(MXS_SESSION* session, mxs::Component* component)
    : m_downstream(component)
    , m_session(session)
    , m_session_data(static_cast<MYSQL_session*>(session->protocol_data()))
    , m_version(service_get_version(session->service, SERVICE_VERSION_MIN))
    , m_qc(MariaDBParser::get(), session)
{
    m_track_pooling_status = session->idle_pooling_enabled();
}

void MariaDBClientConnection::execute_kill(std::shared_ptr<KillInfo> info, std::function<void()> kill_resp)
{
    MXS_SESSION* ref = session_get_ref(m_session);
    auto origin = mxs::RoutingWorker::get_current();

    auto search_connections_and_kill =
        [this, info = std::move(info), ref, origin, kill_resp = std::move(kill_resp)]() {
        // First, gather the list of servers where the KILL should be sent
        auto search_targets = [info]() {
            info->generate_target_list(mxs::RoutingWorker::get_current());
        };
        mxs::RoutingWorker::execute_concurrently(search_targets);

        // TODO: This doesn't handle the case where a session is moved from one worker to another while
        // this was being executed on the MainWorker.
        auto kill_connections = [this, info, ref, send_kill_resp = std::move(kill_resp)]() {
            MXS_SESSION::Scope scope(m_session);

            for (const auto& a : info->targets)
            {
                std::unique_ptr<LocalClient> client(LocalClient::create(info->session, a));

                if (client)
                {
                    try
                    {
                        client->connect();
                        auto ok_cb = [this, send_kill_resp, cl = client.get()](
                            GWBUF&& buf, const mxs::ReplyRoute& route, const mxs::Reply& reply){
                            MXB_INFO("Reply to KILL from '%s': %s",
                                     route.empty() ? "<none>" : route.first()->target()->name(),
                                     reply.error() ? reply.error().message().c_str() : "OK");
                            kill_complete(send_kill_resp, cl);
                        };
                        auto err_cb = [this, send_kill_resp, cl = client.get()](
                            const std::string& err, mxs::Target* tgt, const mxs::Reply& reply) {
                            MXB_INFO("KILL error on '%s': %s", tgt->name(), err.c_str());
                            kill_complete(send_kill_resp, cl);
                        };

                        client->set_notify(std::move(ok_cb), std::move(err_cb));

                        // TODO: There can be multiple connections to the same server. Currently only
                        // one connection per server is killed.

                        string kill_query = info->generate_kill_query(a);
                        MXB_INFO("KILL on '%s': %s", a->name(), kill_query.c_str());

                        if (!client->queue_query(mariadb::create_query(kill_query)))
                        {
                            MXB_INFO("Failed to route KILL query to '%s'", a->name());
                        }
                        else
                        {
                            mxb_assert(ref->state() != MXS_SESSION::State::STOPPING);
                            add_local_client(client.release());
                        }
                    }
                    catch (const mxb::Exception& e)
                    {
                        MXB_INFO("Failed to connect LocalClient to '%s': %s", a->name(), e.what());
                    }
                }
                else
                {
                    MXB_INFO("Failed to create LocalClient to '%s'", a->name());
                }
            }

            // If we ended up not sending any KILL commands, the OK packet can be generated immediately.
            maybe_send_kill_response(send_kill_resp);

            // The reference can now be freed as the execution is back on the worker that owns it
            session_put_ref(ref);
        };

        // Then move execution back to the original worker to keep all connections on the same thread
        origin->execute(kill_connections, mxs::RoutingWorker::EXECUTE_AUTO);
    };

    if (!mxs::MainWorker::get()->execute(search_connections_and_kill, mxb::Worker::EXECUTE_QUEUED))
    {
        session_put_ref(ref);
        m_session->kill();
    }
}

std::string kill_query_prefix(MariaDBClientConnection::kill_type_t type)
{
    using Type = MariaDBClientConnection::kill_type_t;
    const char* hard = type & Type::KT_HARD ? "HARD " : (type & Type::KT_SOFT ? "SOFT " : "");
    const char* query = type & Type::KT_QUERY ? "QUERY " : "";
    std::stringstream ss;
    ss << "KILL " << hard << query;
    return ss.str();
}

void MariaDBClientConnection::mxs_mysql_execute_kill(uint64_t target_id,
                                                     MariaDBClientConnection::kill_type_t type,
                                                     std::function<void()> cb)
{
    auto str = kill_query_prefix(type);
    auto info = std::make_shared<ConnKillInfo>(target_id, str, m_session);
    execute_kill(std::move(info), std::move(cb));
}

/**
 * Send KILL to all but the keep_protocol_thread_id. If keep_protocol_thread_id==0, kill all.
 * TODO: The naming: issuer, target_id, protocol_thread_id is not very descriptive,
 *       and really goes to the heart of explaining what the session_id/thread_id means in terms
 *       of a service/server pipeline and the recursiveness of this call.
 */
void MariaDBClientConnection::execute_kill_connection(uint64_t target_id,
                                                      MariaDBClientConnection::kill_type_t type)
{
    auto str = kill_query_prefix(type);
    auto info = std::make_shared<ConnKillInfo>(target_id, str, m_session);
    execute_kill(std::move(info), std::bind(&MariaDBClientConnection::send_ok_for_kill, this));
}

void MariaDBClientConnection::execute_kill_user(const char* user, kill_type_t type)
{
    auto str = kill_query_prefix(type);
    str += "USER ";
    str += user;

    auto info = std::make_shared<UserKillInfo>(user, str, m_session);
    execute_kill(std::move(info), std::bind(&MariaDBClientConnection::send_ok_for_kill, this));
}

void MariaDBClientConnection::send_ok_for_kill()
{
    // Check if the DCB is still open. If MaxScale is shutting down, the DCB is
    // already closed when this callback is called and an error about a write to a
    // closed DCB would be logged.
    if (m_dcb->is_open())
    {
        write_ok_packet(1);
    }
}

std::string MariaDBClientConnection::current_db() const
{
    return m_session_data->current_db;
}

const MariaDBUserCache* MariaDBClientConnection::user_account_cache()
{
    auto users = m_session->service->user_account_cache();
    return static_cast<const MariaDBUserCache*>(users);
}

bool MariaDBClientConnection::parse_ssl_request_packet(const GWBUF& buffer)
{
    size_t len = buffer.length();
    // The packet length should be exactly header + 32 = 36 bytes.
    bool rval = false;
    if (len == MYSQL_AUTH_PACKET_BASE_SIZE)
    {
        packet_parser::ByteVec data;
        data.resize(CLIENT_CAPABILITIES_LEN);
        buffer.copy_data(MYSQL_HEADER_LEN, CLIENT_CAPABILITIES_LEN, data.data());
        auto res = packet_parser::parse_client_capabilities(data, m_session_data->client_caps);
        m_session_data->client_caps = res.capabilities;
        m_session_data->auth_data->collation = res.collation;
        rval = true;
    }
    return rval;
}

bool MariaDBClientConnection::parse_handshake_response_packet(const GWBUF& buffer)
{
    size_t buflen = buffer.length();
    bool rval = false;

    /**
     * The packet should contain client capabilities at the beginning. Some other fields are also
     * obligatory, so length should be at least 38 bytes. Likely there is more.
     */
    if ((buflen >= NORMAL_HS_RESP_MIN_SIZE) && buflen <= NORMAL_HS_RESP_MAX_SIZE)
    {
        int datalen = buflen - MYSQL_HEADER_LEN;
        packet_parser::ByteVec data;
        data.resize(datalen + 1);
        buffer.copy_data(MYSQL_HEADER_LEN, datalen, data.data());
        data[datalen] = '\0';   // Simplifies some later parsing.

        auto client_info = packet_parser::parse_client_capabilities(data, m_session_data->client_caps);
        auto parse_res = packet_parser::parse_client_response(data,
                                                              client_info.capabilities.basic_capabilities);

        if (parse_res.success)
        {
            // If the buffer is valid, just one 0 should remain. Some (old) connectors may send malformed
            // packets with extra data. Such packets work, but some data may not be parsed properly.
            auto data_size = data.size();
            if (data_size >= 1)
            {
                // Success, save data to session.
                auto& auth_data = *m_session_data->auth_data;
                auth_data.user = move(parse_res.username);
                m_session->set_user(auth_data.user);
                auth_data.client_token = move(parse_res.token_res.auth_token);
                auth_data.default_db = move(parse_res.db);
                auth_data.plugin = move(parse_res.plugin);
                auth_data.collation = client_info.collation;

                // Discard the attributes if there is any indication of failed parsing, as the contents
                // may be garbled.
                if (parse_res.success && data_size == 1)
                {
                    auth_data.attributes = move(parse_res.attr_res.attr_data);
                    MXB_INFO("Connection attributes: %s", attr_to_str(auth_data.attributes).c_str());
                }
                else
                {
                    client_info.capabilities.basic_capabilities &= ~GW_MYSQL_CAPABILITIES_CONNECT_ATTRS;
                }
                m_session_data->client_caps = client_info.capabilities;

                rval = true;
            }
        }
        else if (parse_res.token_res.old_protocol)
        {
            MXB_ERROR("Client %s@%s attempted to connect with pre-4.1 authentication "
                      "which is not supported.", parse_res.username.c_str(), m_dcb->remote().c_str());
        }
    }
    return rval;
}

bool MariaDBClientConnection::require_ssl() const
{
    return m_session->listener_data()->m_ssl.valid();
}

std::tuple<MariaDBClientConnection::StateMachineRes, GWBUF> MariaDBClientConnection::read_ssl_request()
{
    auto rval_res = StateMachineRes::ERROR;
    GWBUF rval_buf;
    const int expected_seq = 1;
    /**
     * Client should have sent an SSLRequest (36 bytes). Client may also have already sent SSL-specific data
     * after the protocol packet. This data should not be read out of the socket, as SSL_accept() will
     * expect to read it.
     */
    auto [read_ok, buffer] = m_dcb->read_strict(MYSQL_HEADER_LEN, SSL_REQUEST_PACKET_SIZE);
    if (!buffer.empty())
    {
        auto header = mariadb::get_header(buffer.data());
        m_next_sequence = header.seq + 1;
        int prot_packet_len = MYSQL_HEADER_LEN + header.pl_length;
        if (prot_packet_len == SSL_REQUEST_PACKET_SIZE)
        {
            if (header.seq == expected_seq)
            {
                if (buffer.length() == SSL_REQUEST_PACKET_SIZE)
                {
                    // Entire packet was available, return it.
                    rval_res = StateMachineRes::DONE;
                    rval_buf = std::move(buffer);
                }
                else
                {
                    // Not enough, unread and wait for more.
                    m_dcb->unread(std::move(buffer));
                    rval_res = StateMachineRes::IN_PROGRESS;
                }
            }
            else
            {
                send_mysql_err_packet(ER_OUT_OF_ORDER, HANDSHAKE_ERRSTATE, PACKETS_OOO_MSG);
                MXB_ERROR(WRONG_SEQ_FMT, m_session_data->remote.c_str(), expected_seq, header.seq);
            }
        }
        else if (prot_packet_len >= NORMAL_HS_RESP_MIN_SIZE)
        {
            // Looks like client is trying to authenticate without ssl. To match server behavior, read the
            // normal handshake and return it. Caller will then send the authentication error to client.
            m_dcb->unread(std::move(buffer));
            auto [hs_res, hs_buffer] = read_handshake_response(expected_seq);
            rval_res = hs_res;
            if (hs_res == StateMachineRes::DONE)
            {
                rval_buf = std::move(hs_buffer);
            }
        }
        else
        {
            send_mysql_err_packet(ER_BAD_HANDSHAKE, HANDSHAKE_ERRSTATE, BAD_SSL_HANDSHAKE_MSG);
            MXB_ERROR(BAD_SSL_HANDSHAKE_FMT, m_dcb->remote().c_str());
        }
    }
    else if (read_ok)
    {
        // Not even the header was available, read again.
        rval_res = StateMachineRes::IN_PROGRESS;
    }
    return {rval_res, std::move(rval_buf)};
}

std::tuple<MariaDBClientConnection::StateMachineRes, GWBUF>
MariaDBClientConnection::read_handshake_response(int expected_seq)
{
    auto rval_res = StateMachineRes::ERROR;
    GWBUF rval_buf;

    // Expecting a normal HandshakeResponse.
    auto [read_ok, buffer] = m_dcb->read(MYSQL_HEADER_LEN, NORMAL_HS_RESP_MAX_SIZE);
    if (!buffer.empty())
    {
        auto header = mariadb::get_header(buffer.data());
        size_t prot_packet_len = MYSQL_HEADER_LEN + header.pl_length;
        m_next_sequence = header.seq + 1;
        if (prot_packet_len >= NORMAL_HS_RESP_MIN_SIZE && prot_packet_len <= NORMAL_HS_RESP_MAX_SIZE)
        {
            if (header.seq == expected_seq)
            {
                if (buffer.length() >= prot_packet_len)
                {
                    // Entire packet was available, return it.
                    rval_buf = buffer.split(prot_packet_len);
                    rval_res = StateMachineRes::DONE;

                    if (!buffer.empty())
                    {
                        // More than the packet was available. Unexpected but allowed.
                        m_dcb->unread(std::move(buffer));
                        m_dcb->trigger_read_event();
                    }
                }
                else
                {
                    // Not enough, unread and wait for more.
                    m_dcb->unread(std::move(buffer));
                    rval_res = StateMachineRes::IN_PROGRESS;
                }
            }
            else
            {
                send_mysql_err_packet(ER_OUT_OF_ORDER, HANDSHAKE_ERRSTATE, PACKETS_OOO_MSG);
                MXB_ERROR(WRONG_SEQ_FMT, m_session_data->remote.c_str(), expected_seq, header.seq);
            }
        }
        else
        {
            send_mysql_err_packet(ER_BAD_HANDSHAKE, HANDSHAKE_ERRSTATE, BAD_HANDSHAKE_MSG);
            if (prot_packet_len > NORMAL_HS_RESP_MAX_SIZE)
            {
                // Unexpected. The HandshakeResponse should not be needlessly large. The limit may need to be
                // changed in case some connector sends large responses. Still, limiting the amount is useful
                // as the client is not yet authenticated and can be malicious.
                MXB_ERROR("Client (%s) tried to send a large HandshakeResponse (%zu bytes).",
                          m_session_data->remote.c_str(), prot_packet_len);
            }
            else
            {
                MXB_ERROR(BAD_HANDSHAKE_FMT, m_session_data->remote.c_str());
            }
        }
    }
    else if (read_ok)
    {
        // Not even the header was available, read again.
        rval_res = StateMachineRes::IN_PROGRESS;
    }
    return {rval_res, std::move(rval_buf)};
}

void MariaDBClientConnection::wakeup()
{
    mxb_assert(m_auth_state == AuthState::TRY_AGAIN);
    m_user_update_wakeup = true;
    m_dcb->trigger_read_event();
}

bool MariaDBClientConnection::is_movable() const
{
    mxb_assert(mxs::RoutingWorker::get_current() == m_dcb->polling_worker());
    return m_auth_state != AuthState::TRY_AGAIN && m_auth_state != AuthState::FIND_ENTRY_RDNS
           && m_auth_state != AuthState::TRY_AGAIN_RDNS;
}

bool MariaDBClientConnection::is_idle() const
{
    return in_routing_state() && m_num_responses == 0;
}

size_t MariaDBClientConnection::sizeof_buffers() const
{
    size_t rv = ClientConnectionBase::sizeof_buffers();

    return rv;
}

bool MariaDBClientConnection::safe_to_restart() const
{
    return !m_session_data->is_trx_active() && !m_session_data->is_trx_ending();
}

bool MariaDBClientConnection::start_change_user(GWBUF&& buffer)
{
    // Parse the COM_CHANGE_USER-packet. The packet is somewhat similar to a typical handshake response.
    size_t buflen = buffer.length();
    bool rval = false;

    size_t min_expected_len = MYSQL_HEADER_LEN + 5;
    auto max_expected_len = min_expected_len + MYSQL_USER_MAXLEN + MYSQL_DATABASE_MAXLEN + 1000;
    if ((buflen >= min_expected_len) && buflen <= max_expected_len)
    {
        int datalen = buflen - MYSQL_HEADER_LEN;
        packet_parser::ByteVec data;
        data.resize(datalen + 1);
        buffer.copy_data(MYSQL_HEADER_LEN, datalen, data.data());
        data[datalen] = '\0';   // Simplifies some later parsing.

        auto parse_res = packet_parser::parse_change_user_packet(data, m_session_data->client_capabilities());
        if (parse_res.success)
        {
            // Only the last byte should be left.
            if (data.size() == 1)
            {
                m_change_user.client_query = move(buffer);

                // Use alternate authentication data storage during change user processing. The effects are
                // not visible to the session. The client authenticator object does not need to be preserved.
                m_change_user.auth_data = std::make_unique<mariadb::AuthenticationData>();
                auto& auth_data = *m_change_user.auth_data;
                auth_data.user = move(parse_res.username);
                auth_data.default_db = move(parse_res.db);
                auth_data.plugin = move(parse_res.plugin);
                auth_data.collation = parse_res.charset;
                auth_data.client_token = move(parse_res.token_res.auth_token);
                auth_data.attributes = move(parse_res.attr_res.attr_data);

                rval = true;
                MXB_INFO("Client %s is attempting a COM_CHANGE_USER to '%s'. Connection attributes: %s",
                         m_session_data->user_and_host().c_str(), auth_data.user.c_str(),
                         attr_to_str(auth_data.attributes).c_str());
            }
        }
        else if (parse_res.token_res.old_protocol)
        {
            MXB_ERROR("Client %s attempted a COM_CHANGE_USER with pre-4.1 authentication, "
                      "which is not supported.", m_session_data->user_and_host().c_str());
        }
    }
    return rval;
}

bool MariaDBClientConnection::complete_change_user_p1()
{
    // Change-user succeeded on client side. It must still be routed to backends and the reply needs to
    // be OK. Either can fail. First, backup current session authentication data, then overwrite it with
    // the change-user authentication data. Backend authenticators will read the new data.

    auto& curr_auth_data = m_session_data->auth_data;
    m_change_user.auth_data_bu = move(curr_auth_data);
    curr_auth_data = move(m_change_user.auth_data);

    assign_backend_authenticator(*curr_auth_data);

    bool rval = false;
    // Failure here means a comms error -> session failure.
    if (route_statement(move(m_change_user.client_query)))
    {
        m_routing_state = RoutingState::CHANGING_STATE;
        m_changing_state = ChangingState::USER;
        rval = true;
    }
    m_change_user.client_query.clear();
    return rval;
}

void MariaDBClientConnection::cancel_change_user_p1()
{
    MXB_INFO("COM_CHANGE_USER from '%s' to '%s' failed.",
             m_session_data->auth_data->user.c_str(), m_change_user.auth_data->user.c_str());
    // The main session fields have not been modified at this point, so canceling is simple.
    m_change_user.client_query.clear();
    m_change_user.auth_data.reset();
}

void MariaDBClientConnection::complete_change_user_p2()
{
    // At this point, the original auth data is in backup storage and the change-user data is "current".
    const auto& curr_auth_data = m_session_data->auth_data;
    const auto& orig_auth_data = m_change_user.auth_data_bu;

    if (curr_auth_data->user_entry.entry.super_priv && mxs::Config::get().log_warn_super_user)
    {
        MXB_WARNING("COM_CHANGE_USER from '%s' to super user '%s'.",
                    orig_auth_data->user.c_str(), curr_auth_data->user.c_str());
    }
    else
    {
        MXB_INFO("COM_CHANGE_USER from '%s' to '%s' succeeded.",
                 orig_auth_data->user.c_str(), curr_auth_data->user.c_str());
    }
    m_change_user.auth_data_bu.reset();     // No longer needed.
    m_session_data->current_db = curr_auth_data->default_db;
    m_session_data->role = curr_auth_data->user_entry.entry.default_role;
}

void MariaDBClientConnection::cancel_change_user_p2(const GWBUF& buffer)
{
    auto& curr_auth_data = m_session_data->auth_data;
    auto& orig_auth_data = m_change_user.auth_data_bu;

    MXB_WARNING("COM_CHANGE_USER from '%s' to '%s' succeeded on MaxScale but "
                "returned (0x%0hhx) on backends: %s",
                orig_auth_data->user.c_str(), curr_auth_data->user.c_str(),
                mariadb::get_command(buffer), mariadb::extract_error(buffer).c_str());

    // Restore original auth data from backup.
    curr_auth_data = move(orig_auth_data);
}

MariaDBClientConnection::StateMachineRes MariaDBClientConnection::process_handshake()
{
    auto rval = StateMachineRes::IN_PROGRESS;   // Returned to upper level SM
    bool state_machine_continue = true;

    while (state_machine_continue)
    {
        switch (m_handshake_state)
        {
        case HSState::INIT:
            {
                m_session_data->auth_data = std::make_unique<mariadb::AuthenticationData>();
                m_next_sequence = 1;    // Handshake had seq 0 so any errors will have 1.
                // If proxy protocol is not enabled at all (typical case), skip the proxy header read phase.
                // This may save an io-op.
                bool proxyproto_on = !m_session->listener_data()->m_proxy_networks.empty();
                m_handshake_state = proxyproto_on ? HSState::EXPECT_PROXY_HDR :
                    (require_ssl() ? HSState::EXPECT_SSL_REQ : HSState::EXPECT_HS_RESP);
            }
            break;

        case HSState::EXPECT_PROXY_HDR:
            {
                // Even if proxy protocol is not allowed for this specific client, try to read the header.
                // Allows more descriptive error messages for clients trying to proxy.
                bool ssl_on = require_ssl();
                auto proxy_read_res = read_proxy_header(ssl_on);
                if (proxy_read_res == StateMachineRes::DONE)
                {
                    m_handshake_state = ssl_on ? HSState::EXPECT_SSL_REQ : HSState::EXPECT_HS_RESP;
                }
                else if (proxy_read_res == StateMachineRes::IN_PROGRESS)
                {
                    // The total proxy header was not available, check again later.
                    state_machine_continue = false;
                }
                else
                {
                    m_handshake_state = HSState::FAIL;
                }
            }
            break;

        case HSState::EXPECT_SSL_REQ:
            {
                auto [res, buffer] = read_ssl_request();
                if (res == StateMachineRes::DONE)
                {
                    // Sequence has already been checked.
                    if (parse_ssl_request_packet(buffer))
                    {
                        m_handshake_state = HSState::SSL_NEG;
                    }
                    else if (parse_handshake_response_packet(buffer))
                    {
                        // Trying to log in without ssl. Server sends this error when a user account requires
                        // ssl but client is not using it.
                        send_authentication_error(AuthErrorType::ACCESS_DENIED);
                        MXB_INFO("Client %s tried to log in without SSL when listener '%s' is configured to "
                                 "require it.", m_session->user_and_host().c_str(),
                                 m_session->listener_data()->m_listener_name.c_str());
                        m_handshake_state = HSState::FAIL;
                    }
                    else
                    {
                        send_mysql_err_packet(ER_BAD_HANDSHAKE, HANDSHAKE_ERRSTATE, BAD_SSL_HANDSHAKE_MSG);
                        MXB_ERROR(BAD_SSL_HANDSHAKE_FMT, m_dcb->remote().c_str());
                        m_handshake_state = HSState::FAIL;
                    }
                }
                else if (res == StateMachineRes::IN_PROGRESS)
                {
                    state_machine_continue = false;
                }
                else
                {
                    m_handshake_state = HSState::FAIL;
                }
            }
            break;

        case HSState::SSL_NEG:
            {
                // Client should be negotiating ssl.
                auto ssl_status = ssl_authenticate_check_status();
                if (ssl_status == SSLState::COMPLETE)
                {
                    m_handshake_state = HSState::EXPECT_HS_RESP;
                    m_session_data->client_conn_encrypted = true;
                }
                else if (ssl_status == SSLState::INCOMPLETE)
                {
                    // SSL negotiation should complete in the background. Execution returns here once
                    // complete.
                    state_machine_continue = false;
                }
                else
                {
                    write(mariadb::create_error_packet(m_next_sequence, 1045, "28000", "Access without SSL denied"));
                    MXB_ERROR("Client (%s) failed SSL negotiation.", m_session_data->remote.c_str());
                    m_handshake_state = HSState::FAIL;
                }
            }
            break;

        case HSState::EXPECT_HS_RESP:
            {
                // Expecting normal Handshake response.
                // @see https://mariadb.com/kb/en/library/connection/#client-handshake-response
                int expected_seq = require_ssl() ? 2 : 1;
                auto [res, buffer] = read_handshake_response(expected_seq);
                if (res == StateMachineRes::DONE)
                {
                    // Packet length and sequence already checked.
                    if (parse_handshake_response_packet(buffer))
                    {
                        m_handshake_state = HSState::COMPLETE;
                    }
                    else
                    {
                        send_mysql_err_packet(ER_BAD_HANDSHAKE, HANDSHAKE_ERRSTATE, BAD_HANDSHAKE_MSG);
                        MXB_ERROR(BAD_HANDSHAKE_FMT, m_session_data->remote.c_str());
                        m_handshake_state = HSState::FAIL;
                    }
                }
                else if (res == StateMachineRes::IN_PROGRESS)
                {
                    state_machine_continue = false;
                }
                else
                {
                    m_handshake_state = HSState::FAIL;
                }
            }
            break;

        case HSState::COMPLETE:
            state_machine_continue = false;
            rval = StateMachineRes::DONE;
            break;

        case HSState::FAIL:
            // An error message should have already been sent.
            state_machine_continue = false;
            rval = StateMachineRes::ERROR;
            break;
        }
    }
    return rval;
}

void MariaDBClientConnection::send_authentication_error(AuthErrorType error, const std::string& auth_mod_msg)
{
    auto ses = m_session_data;
    string mariadb_msg;
    const auto& auth_data = *ses->auth_data;

    switch (error)
    {
    case AuthErrorType::ACCESS_DENIED:
        mariadb_msg = mxb::string_printf("Access denied for user %s (using password: %s)",
                                         ses->user_and_host().c_str(),
                                         auth_data.client_token.empty() ? "NO" : "YES");
        send_mysql_err_packet(1045, "28000", mariadb_msg.c_str());
        break;

    case AuthErrorType::DB_ACCESS_DENIED:
        mariadb_msg = mxb::string_printf("Access denied for user %s to database '%s'",
                                         ses->user_and_host().c_str(), auth_data.default_db.c_str());
        send_mysql_err_packet(1044, "42000", mariadb_msg.c_str());
        break;

    case AuthErrorType::BAD_DB:
        mariadb_msg = mxb::string_printf("Unknown database '%s'", auth_data.default_db.c_str());
        send_mysql_err_packet(1049, "42000", mariadb_msg.c_str());
        break;

    case AuthErrorType::NO_PLUGIN:
        mariadb_msg = mxb::string_printf("Plugin '%s' is not loaded",
                                         auth_data.user_entry.entry.plugin.c_str());
        send_mysql_err_packet(1524, "HY000", mariadb_msg.c_str());
        break;
    }

    // Also log an authentication failure event.
    if (m_session->service->config()->log_auth_warnings)
    {
        string total_msg = mxb::string_printf("Authentication failed for user '%s'@[%s] to service '%s'. "
                                              "Originating listener: '%s'. MariaDB error: '%s'.",
                                              auth_data.user.c_str(), ses->remote.c_str(),
                                              m_session->service->name(),
                                              m_session->listener_data()->m_listener_name.c_str(),
                                              mariadb_msg.c_str());
        if (!auth_mod_msg.empty())
        {
            total_msg += mxb::string_printf(" Authenticator error: '%s'.", auth_mod_msg.c_str());
        }
        MXS_LOG_EVENT(maxscale::event::AUTHENTICATION_FAILURE, "%s", total_msg.c_str());
    }
}

void MariaDBClientConnection::send_misc_error(const std::string& msg)
{
    send_mysql_err_packet(1105, "HY000", msg.c_str());
}

void MariaDBClientConnection::trigger_ext_auth_exchange()
{
    auto& auth_data = (m_state == State::CHANGING_USER) ? *m_change_user.auth_data :
        *m_session_data->auth_data;
    auto adv_state = perform_auth_exchange(GWBUF(), auth_data);
    if (adv_state)
    {
        m_dcb->trigger_read_event();    // Get the main state machine to advance.
    }
}

bool MariaDBClientConnection::read_and_auth_exchange(mariadb::AuthenticationData& auth_data)
{
    mxb_assert(m_auth_state == AuthState::CONTINUE_EXCHANGE);
    auto [read_ok, buffer] = read_protocol_packet();
    bool advance_state = false;
    if (buffer.empty())
    {
        if (!read_ok)
        {
            // Connection is likely broken, no need to send error message.
            m_auth_state = AuthState::FAIL;
            advance_state = true;
        }
    }
    else
    {
        advance_state = perform_auth_exchange(std::move(buffer), auth_data);
    }
    return advance_state;
}
/**
 * Authentication exchange state for authenticator state machine.
 *
 * @return True, if the calling state machine should continue. False, if it should wait for more client data.
 */
bool MariaDBClientConnection::perform_auth_exchange(GWBUF&& buffer, mariadb::AuthenticationData& auth_data)
{
    mxb_assert(m_auth_state == AuthState::START_EXCHANGE || m_auth_state == AuthState::CONTINUE_EXCHANGE);

    auto res = m_authenticator->exchange(move(buffer), m_session_data, auth_data);
    if (!res.packet.empty())
    {
        mxb_assert(res.packet.is_unique());
        res.packet.data()[MYSQL_SEQ_OFFSET] = m_next_sequence;
        write(move(res.packet));
    }

    bool state_machine_continue = true;
    if (res.status == ExcRes::Status::READY)
    {
        // Continue to password check.
        m_auth_state = AuthState::CHECK_TOKEN;
    }
    else if (res.status == ExcRes::Status::INCOMPLETE)
    {
        // Authentication is expecting another packet from client, so jump out.
        if (m_auth_state == AuthState::START_EXCHANGE)
        {
            m_auth_state = AuthState::CONTINUE_EXCHANGE;
        }
        state_machine_continue = false;
    }
    else
    {
        // Exchange failed. Usually a communication or memory error.
        auto msg = mxb::string_printf("Authentication plugin '%s' failed",
                                      auth_data.client_auth_module->name().c_str());
        send_misc_error(msg);
        m_auth_state = AuthState::FAIL;
    }
    return state_machine_continue;
}

void MariaDBClientConnection::perform_check_token(AuthType auth_type)
{
    // If the user entry didn't exist in the first place, don't check token and just fail.
    // TODO: server likely checks some random token to spend time, could add it later.
    auto& auth_data = authentication_data(auth_type);
    const auto& user_entry = auth_data.user_entry;
    const auto entrytype = user_entry.type;

    if (entrytype == UserEntryType::USER_NOT_FOUND)
    {
        send_authentication_error(AuthErrorType::ACCESS_DENIED);
        m_auth_state = AuthState::FAIL;
    }
    else
    {
        AuthRes auth_val;
        auto& sett = m_session_data->user_search_settings.listener;
        if (sett.check_password && !sett.passthrough_auth)
        {
            auth_val = m_authenticator->authenticate(m_session_data, auth_data);
        }
        else
        {
            auth_val.status = AuthRes::Status::SUCCESS;
        }

        if (auth_val.status == AuthRes::Status::SUCCESS)
        {
            if (entrytype == UserEntryType::USER_ACCOUNT_OK)
            {
                // Authentication succeeded. If the user has super privileges, print a warning. The change-
                // user equivalent is printed elsewhere.
                if (auth_type == AuthType::NORMAL_AUTH)
                {
                    m_auth_state = AuthState::START_SESSION;
                    if (user_entry.entry.super_priv && mxs::Config::get().log_warn_super_user)
                    {
                        MXB_WARNING("Super user %s logged in to service '%s'.",
                                    m_session_data->user_and_host().c_str(), m_session->service->name());
                    }
                }
                else
                {
                    m_auth_state = AuthState::CHANGE_USER_OK;
                }
            }
            else
            {
                // Translate the original user account search error type to an error message type.
                auto error = AuthErrorType::ACCESS_DENIED;
                switch (entrytype)
                {
                case UserEntryType::DB_ACCESS_DENIED:
                    error = AuthErrorType::DB_ACCESS_DENIED;
                    break;

                case UserEntryType::ROOT_ACCESS_DENIED:
                case UserEntryType::ANON_PROXY_ACCESS_DENIED:
                    error = AuthErrorType::ACCESS_DENIED;
                    break;

                case UserEntryType::BAD_DB:
                    error = AuthErrorType::BAD_DB;
                    break;

                default:
                    mxb_assert(!true);
                }
                send_authentication_error(error, auth_val.msg);
                m_auth_state = AuthState::FAIL;
            }
        }
        else
        {
            if (auth_val.status == AuthRes::Status::FAIL_WRONG_PW
                && user_account_cache()->can_update_immediately())
            {
                // Again, this may be because user data is obsolete. Update userdata, but fail
                // session anyway since I/O with client cannot be redone.
                m_session->service->request_user_account_update();
            }
            // This is also sent if the auth module fails.
            send_authentication_error(AuthErrorType::ACCESS_DENIED, auth_val.msg);
            m_auth_state = AuthState::FAIL;
        }
    }

    if (m_auth_state == AuthState::FAIL)
    {
        // Add only the true authentication failures into listener's host blocking counters. This way internal
        // reasons (e.g. no valid master found) don't trigger blocking of hosts.
        mxs::Listener::mark_auth_as_failed(m_dcb->remote());
        m_session->service->stats().add_failed_auth();
    }
}

bool MariaDBClientConnection::in_routing_state() const
{
    return m_state == State::READY;
}

json_t* MariaDBClientConnection::diagnostics() const
{
    json_t* js = json_object();
    json_object_set_new(js, "cipher", json_string(m_dcb->ssl_cipher().c_str()));
    mxb_assert(m_session_data);

    json_t* attrs = m_session_data->auth_data ?
        attr_to_json(m_session_data->auth_data->attributes) : json_null();
    json_object_set_new(js, "connection_attributes", attrs);

    if (m_session->capabilities() & RCAP_TYPE_SESCMD_HISTORY)
    {
        m_session_data->history().fill_json(js);
        json_object_set_new(js, "sescmd_history_stored_metadata",
                            json_integer(m_session_data->exec_metadata.size()));
    }

    return js;
}

bool MariaDBClientConnection::large_query_continues(const GWBUF& buffer) const
{
    return mariadb::get_header(buffer.data()).pl_length == MYSQL_PACKET_LENGTH_MAX;
}

bool MariaDBClientConnection::process_normal_packet(GWBUF&& buffer)
{
    bool success = false;
    bool is_large = false;
    {
        const uint8_t* data = buffer.data();
        auto header = mariadb::get_header(data);
        m_command = mariadb::get_command(data);
        is_large = (header.pl_length == MYSQL_PACKET_LENGTH_MAX);
    }

    switch (m_command)
    {
    case MXS_COM_CHANGE_USER:
        // Client sent a change-user-packet. Parse it but only route it once change-user completes.
        if (start_change_user(move(buffer)))
        {
            m_state = State::CHANGING_USER;
            m_auth_state = AuthState::FIND_ENTRY;
            m_dcb->trigger_read_event();
            success = true;
        }
        break;

    case MXS_COM_QUIT:
        /** The client is closing the connection. We know that this will be the
         * last command the client sends so the backend connections are very likely
         * to be in an idle state.
         *
         * If the client is pipelining the queries (i.e. sending N request as
         * a batch and then expecting N responses) then it is possible that
         * the backend connections are not idle when the COM_QUIT is received.
         * In most cases we can assume that the connections are idle. */
        m_session->set_can_pool_backends(true);
        m_session->set_normal_quit();
        success = route_statement(move(buffer));
        break;

    case MXS_COM_SET_OPTION:
        /**
         * This seems to be only used by some versions of PHP.
         *
         * The option is stored as a two byte integer with the values 0 for enabling
         * multi-statements and 1 for disabling it.
         */
        {
            auto& caps = m_session_data->client_caps.basic_capabilities;
            if (buffer.data()[MYSQL_HEADER_LEN + 2])
            {
                caps &= ~GW_MYSQL_CAPABILITIES_MULTI_STATEMENTS;
            }
            else
            {
                caps |= GW_MYSQL_CAPABILITIES_MULTI_STATEMENTS;
            }
            success = route_statement(move(buffer));
        }

        break;

    case MXS_COM_PROCESS_KILL:
        {
            const uint8_t* data = buffer.data();
            uint64_t process_id = mariadb::get_byte4(data + MYSQL_HEADER_LEN + 1);
            execute_kill_connection(process_id, KT_CONNECTION);
            success = true;     // No further processing or routing.
        }
        break;

    case MXS_COM_INIT_DB:
        {
            const uint8_t* data = buffer.data();
            auto start = data + MYSQL_HEADER_LEN + 1;
            auto end = data + buffer.length();
            start_change_db(string(start, end));
            success = route_statement(move(buffer));
        }
        break;

    case MXS_COM_QUERY:
        {
            bool route = true;
            bool inspect = true;

            if (rcap_type_required(m_session->capabilities(), RCAP_TYPE_QUERY_CLASSIFICATION))
            {
                inspect = should_inspect_query(buffer);
            }

            if (inspect)
            {
                // Track MaxScale-specific sql. If the variable setting succeeds, the query is routed normally
                // so that the same variable is visible on backend.
                string errmsg = handle_variables(buffer);
                if (!errmsg.empty())
                {
                    // No need to route the query, send error to client.
                    success = write(mariadb::create_error_packet(1, 1193, "HY000", errmsg.c_str()));
                    route = false;
                }
                // Some queries require special handling. Some of these are text versions of other
                // similarly handled commands.
                else if (process_special_queries(buffer) == SpecialCmdRes::END)
                {
                    success = true;     // No need to route query.
                    route = false;
                }
            }

            if (route)
            {
                success = route_statement(move(buffer));
            }
        }
        break;

    case MXS_COM_BINLOG_DUMP:
        if (!m_allow_replication)
        {
            int FEATURE_DISABLED = 1289;
            success = write(mariadb::create_error_packet(
                1, FEATURE_DISABLED, "HY000", "Replication protocol is disabled"));
            break;
        }
        [[fallthrough]];

    default:
        if (mxs_mysql_is_valid_command(m_command))
        {
            // Not a query, just a command which does not require special handling.
            success = route_statement(move(buffer));
        }
        else
        {
            success = write(mariadb::create_error_packet(1, 1047, "08S01", "Unknown command"));
        }
        break;
    }

    if (success && is_large)
    {
        // This will fail on non-routed packets. Such packets would be malformed anyway.
        // TODO: Add a DISCARD_LARGE_PACKET state for discarding the tail end of anything we don't support
        if (m_routing_state == RoutingState::RECORD_HISTORY)
        {
            m_routing_state = RoutingState::LARGE_HISTORY_PACKET;
        }
        else
        {
            m_routing_state = RoutingState::LARGE_PACKET;
        }
    }

    return success;
}

std::map<std::string, std::string> MariaDBClientConnection::get_sysvar_values()
{
    const auto& meta = m_session->connection_metadata();
    std::map<std::string, std::string> rval = meta.metadata;

    auto set_if_found = [&](const auto& key, const auto& value) {
        if (auto it_elem = rval.find(key); it_elem != rval.end())
        {
            it_elem->second = value;
        }
    };

    // We need to replace the character_set_client with the actual character set name.
    mxb_assert(m_session_data->auth_data);
    auto it = meta.collations.find(m_session_data->auth_data->collation);

    if (it != meta.collations.end())
    {
        set_if_found("character_set_client", it->second.character_set);
        set_if_found("character_set_connection", it->second.character_set);
        set_if_found("character_set_results", it->second.character_set);
        set_if_found("collation_connection", it->second.collation);
    }

    rval.emplace("threads_connected", std::to_string(m_session->service->stats().n_client_conns()));
    rval.emplace("connection_id", std::to_string(m_session->id()));

    return rval;
}


void MariaDBClientConnection::write_ok_packet(int sequence, uint8_t affected_rows)
{
    if (m_session_data->client_caps.basic_capabilities & GW_MYSQL_CAPABILITIES_SESSION_TRACK)
    {
        write(mariadb::create_ok_packet(sequence, affected_rows, get_sysvar_values()));
    }
    else
    {
        write(mariadb::create_ok_packet(sequence, affected_rows));
    }
}

bool MariaDBClientConnection::send_mysql_err_packet(int mysql_errno, const char* sqlstate_msg,
                                                    const char* mysql_message)
{
    return write(mariadb::create_error_packet(m_next_sequence, mysql_errno, sqlstate_msg, mysql_message));
}

bool
MariaDBClientConnection::clientReply(GWBUF&& buffer, const mxs::ReplyRoute& down, const mxs::Reply& reply)
{
    if (m_num_responses == 1)
    {
        // First check the changing state, because the routing state need not
        // be CHANGING_STATE but may also be RECORD_HISTORY if the history is
        // being recorded.
        if (m_changing_state != ChangingState::NONE)
        {
            mxb_assert(m_routing_state == RoutingState::CHANGING_STATE
                       || m_routing_state == RoutingState::RECORD_HISTORY);

            switch (m_changing_state)
            {
            case ChangingState::DB:
                if (reply.is_ok())
                {
                    // Database change succeeded.
                    m_session_data->current_db = move(m_pending_value);
                    m_session->notify_userdata_change();
                }
                break;

            case ChangingState::ROLE:
                if (reply.is_ok())
                {
                    // Role change succeeded. Role "NONE" is special, in that it means no role is active.
                    if (m_pending_value == "NONE")
                    {
                        m_session_data->role.clear();
                    }
                    else
                    {
                        m_session_data->role = move(m_pending_value);
                    }
                    m_session->notify_userdata_change();
                }
                break;

            case ChangingState::USER:
                // Route the reply to client. The sequence in the server packet may be wrong, fix it.
                buffer.data()[3] = m_next_sequence;
                if (reply.is_ok())
                {
                    complete_change_user_p2();
                    m_session->notify_userdata_change();
                }
                else
                {
                    // Change user succeeded on MaxScale but failed on backends. Cancel it.
                    cancel_change_user_p2(buffer);
                }
                break;

            case ChangingState::NONE:
                mxb_assert(!true);
                break;
            }

            m_pending_value.clear();
            m_changing_state = ChangingState::NONE;
        }

        switch (m_routing_state)
        {
        case RoutingState::CHANGING_STATE:
            // Regardless of result, state change is complete. Note that the
            // routing state may also be RECORD_HISTORY.
            m_routing_state = RoutingState::PACKET_START;
            m_dcb->trigger_read_event();
            break;

        case RoutingState::LOAD_DATA:
            if (reply.is_complete())
            {
                m_routing_state = RoutingState::PACKET_START;
            }
            break;

        case RoutingState::RECORD_HISTORY:
            finish_recording_history(reply);
            break;

        default:
            if (reply.state() == mxs::ReplyState::LOAD_DATA)
            {
                m_routing_state = RoutingState::LOAD_DATA;
            }
            break;
        }
    }

    if (m_session->capabilities() & RCAP_TYPE_SESCMD_HISTORY)
    {
        m_qc.update_from_reply(reply);
    }

    if (mxs_mysql_is_binlog_dump(m_command))
    {
        // A COM_BINLOG_DUMP is treated as an endless result. Stop counting the expected responses as the data
        // isn't in the normal result format we expect it to be in. The protocol could go into a more special
        // mode to bypass all processing but this alone cuts out most of it.
    }
    else
    {
        if (reply.is_complete() && !reply.error().is_unexpected_error())
        {
            --m_num_responses;
            mxb_assert(m_num_responses >= 0);

            m_session->book_server_response(down.first()->target(), true);
        }

        if (reply.is_ok())
        {
            m_session_data->trx_tracker().fix_trx_state(reply);
        }

        if (m_track_pooling_status && !m_pooling_permanent_disable)
        {
            // TODO: Configurable? Also, must be many other situations where backend conns should not be
            // runtime-pooled.
            if (m_session_data->history().pruned())
            {
                m_pooling_permanent_disable = true;
                m_session->set_can_pool_backends(false);
            }
            else
            {
                bool reply_complete = reply.is_complete();
                bool waiting_response = m_num_responses > 0;
                // Trx status detection is likely lacking.
                bool trx_on = m_session_data->is_trx_active() && !m_session_data->is_trx_ending();
                bool pooling_ok = reply_complete && !waiting_response && !trx_on;
                m_session->set_can_pool_backends(pooling_ok);
            }
        }
    }

    return write(std::move(buffer));
}

void MariaDBClientConnection::add_local_client(LocalClient* client)
{
    // Prune stale LocalClients before adding the new one
    m_local_clients.erase(
        std::remove_if(m_local_clients.begin(), m_local_clients.end(), [](const auto& c) {
        return !c->is_open();
    }), m_local_clients.end());

    m_local_clients.emplace_back(client);
}

void MariaDBClientConnection::kill_complete(const std::function<void()>& send_kill_resp, LocalClient* client)
{
    // This needs to be executed once we return from the clientReply or the handleError callback of the
    // LocalClient.
    auto fn = [=]() {
        MXS_SESSION::Scope scope(m_session);

        auto it = std::remove_if(m_local_clients.begin(), m_local_clients.end(), [&](const auto& c) {
            return c.get() == client;
        });

        // It's possible that both the reponse to the KILL as well as an error occur on the same LocalClient
        // before we end up processing either of the two events. For this reason, the validity of the client
        // must be checked before we invoke the callback, otherwise we risk calling it twice.
        if (it != m_local_clients.end())
        {
            mxb_assert(std::distance(it, m_local_clients.end()) == 1);
            m_local_clients.erase(it, m_local_clients.end());
            maybe_send_kill_response(send_kill_resp);
        }
    };

    m_session->worker()->lcall(fn);
}

void MariaDBClientConnection::maybe_send_kill_response(const std::function<void()>& send_kill_resp)
{
    if (!have_local_clients() && m_session->state() == MXS_SESSION::State::STARTED)
    {
        MXB_INFO("All KILL commands finished");
        send_kill_resp();
    }
}

bool MariaDBClientConnection::have_local_clients()
{
    return std::any_of(m_local_clients.begin(), m_local_clients.end(), std::mem_fn(&LocalClient::is_open));
}

void MariaDBClientConnection::kill(std::string_view errmsg)
{
    m_local_clients.clear();

    if (!errmsg.empty())
    {
        int errnum = 1927;          // This is ER_CONNECTION_KILLED
        write(mariadb::create_error_packet(0, errnum, "HY000", errmsg));
    }
}

mxs::Parser* MariaDBClientConnection::parser()
{
    return &MariaDBParser::get();
}

bool MariaDBClientConnection::module_init()
{
    mxb_assert(this_unit.special_queries_regex.empty());

    /*
     * We need to detect the following queries:
     * 1) USE database
     * 2) SET ROLE { role | NONE }
     * 3) KILL [HARD | SOFT] [CONNECTION | QUERY [ID] ] [thread_id | USER user_name | query_id]
     *
     * Construct one regex which captures all of the above. The "?:" disables capturing for redundant groups.
     * Comments at start are skipped. Executable comments are not parsed.
     */
    const char regex_string[] =
        // <main> captures the entire statement.
        R"((?<main>)"
        // Capture "USE database".
        R"(USE\s+(?<db>\w+))"
        // Capture "SET ROLE role".
        R"(|SET\s+ROLE\s+(?<role>\w+))"
        // Capture KILL ...
        R"(|KILL\s+(?:(?<koption>HARD|SOFT)\s+)?(?:(?<ktype>CONNECTION|QUERY|QUERY\s+ID)\s+)?(?<ktarget>\d+|USER\s+\w+))"
        // End of <main>.
        R"())"
        // Ensure the statement ends nicely. Either subject ends, or a comment begins. This
        // last comment is not properly checked as skipping it is not required.
        R"(\s*(?:;|$|--|#|/\*))";

    bool rval = false;
    mxb::Regex regex(regex_string, PCRE2_CASELESS);
    if (regex.valid())
    {
        this_unit.special_queries_regex = move(regex);
        rval = true;
    }
    else
    {
        MXB_ERROR("Regular expression initialization failed. %s", regex.error().c_str());
    }
    return rval;
}

void MariaDBClientConnection::start_change_role(string&& role)
{
    m_routing_state = RoutingState::CHANGING_STATE;
    m_changing_state = ChangingState::ROLE;
    m_pending_value = move(role);
}

void MariaDBClientConnection::start_change_db(string&& db)
{
    m_routing_state = RoutingState::CHANGING_STATE;
    m_changing_state = ChangingState::DB;
    m_pending_value = move(db);
}

MariaDBClientConnection::SpecialQueryDesc
MariaDBClientConnection::parse_special_query(const char* sql, int len)
{
    SpecialQueryDesc rval;
    const auto& regex = this_unit.special_queries_regex;
    if (regex.match(sql, len))
    {
        // Is a tracked command. Look at the captured parts to figure out which one it is.
        auto main_ind = regex.substring_ind_by_name("main");
        mxb_assert(!main_ind.empty());
        char c = sql[main_ind.begin];
        switch (c)
        {
        case 'K':
        case 'k':
            {
                rval = parse_kill_query_elems(sql);
            }
            break;

        case 'S':
        case 's':
            {
                rval.type = SpecialQueryDesc::Type::SET_ROLE;
                rval.target = regex.substring_by_name(sql, "role");
            }
            break;

        case 'U':
        case 'u':
            rval.type = SpecialQueryDesc::Type::USE_DB;
            rval.target = regex.substring_by_name(sql, "db");
            break;

        default:
            mxb_assert(!true);
        }
    }
    return rval;
}

void MariaDBClientConnection::assign_backend_authenticator(mariadb::AuthenticationData& auth_data)
{
    // If manual mapping is on, search for the current user or their group. If not found or if not in use,
    // use same authenticator as client.
    const auto* listener_data = m_session->listener_data();
    const auto* mapping_info = listener_data->m_mapping_info.get();
    bool user_is_mapped = false;

    if (mapping_info)
    {
        // Mapping is enabled for the listener. First, search based on username, then based on Linux user
        // group. Mapping does not depend on incoming user IP (can be added later if there is demand).
        const string* mapped_user = nullptr;
        const auto& user = auth_data.user;
        const auto& user_map = mapping_info->user_map;
        auto it_u = user_map.find(user);
        if (it_u != user_map.end())
        {
            mapped_user = &it_u->second;
        }
        else
        {
            // Perhaps the mapping is defined through the user's Linux group.
            const auto& group_map = mapping_info->group_map;
            if (!group_map.empty())
            {
                auto userc = user.c_str();

                // getgrouplist accepts a default group which the user is always a member of. Use user id
                // from passwd-structure.
                gid_t user_group = 0;
                if (call_getpwnam_r(userc, user_group))
                {
                    const int N = 100;      // Check at most 100 groups.
                    gid_t user_gids[N];
                    int n_groups = N;   // Input-output param
                    getgrouplist(userc, user_group, user_gids, &n_groups);
                    int found_groups = std::min(n_groups, N);
                    for (int i = 0; i < found_groups; i++)
                    {
                        // The group id:s of the user's groups are in the array. Go through each, get
                        // text-form group name and compare to mapping. Use first match.
                        string group_name;
                        if (call_getgrgid_r(user_gids[i], group_name))
                        {
                            auto it_g = group_map.find(group_name);
                            if (it_g != group_map.end())
                            {
                                mapped_user = &it_g->second;
                                break;
                            }
                        }
                    }
                }
            }
        }

        if (mapped_user)
        {
            // Found a mapped user. Search for credentials. If none found, use defaults.
            const auto& creds = mapping_info->credentials;
            const mxs::ListenerData::UserCreds* found_creds = &default_mapped_creds;
            auto it2 = creds.find(*mapped_user);
            if (it2 != creds.end())
            {
                found_creds = &it2->second;
            }

            // Check that the plugin defined for the user is enabled.
            auto* auth_module = find_auth_module(found_creds->plugin);
            if (auth_module)
            {
                // Found authentication module. Apply mapping.
                auth_data.be_auth_module = auth_module;
                const auto& mapped_pw = found_creds->password;
                MXB_INFO("Incoming user '%s' mapped to '%s' using '%s' with %s.",
                         auth_data.user.c_str(), mapped_user->c_str(), found_creds->plugin.c_str(),
                         mapped_pw.empty() ? "no password" : "password");
                auth_data.user = *mapped_user;      // TODO: save to separate field
                auth_data.backend_token = auth_data.be_auth_module->generate_token(mapped_pw);
                user_is_mapped = true;
            }
            else
            {
                MXB_ERROR("Client %s manually maps to '%s', who uses authenticator plugin '%s'. "
                          "The plugin is not enabled for listener '%s'. Falling back to normal "
                          "authentication.",
                          m_session_data->user_and_host().c_str(), mapped_user->c_str(),
                          found_creds->plugin.c_str(), listener_data->m_listener_name.c_str());
            }
        }
    }

    if (!user_is_mapped)
    {
        // No mapping, use client authenticator.
        auth_data.be_auth_module = auth_data.client_auth_module;
        auto& sett = m_session_data->user_search_settings.listener;
        // If passthrough auth or skip auth is on, authenticate() was not ran.
        if (sett.passthrough_auth)
        {
            // Authenticator should have calculated backend token during exchange().
        }
        else if (!sett.check_password)
        {
            //  Backend token is empty, generate it from the client token.
            auto ptr = (const char*)auth_data.client_token.data();
            auto len = auth_data.client_token.size();
            string temp(ptr, len);
            auth_data.backend_token = auth_data.be_auth_module->generate_token(temp);
            auth_data.backend_token_2fa = auth_data.client_token_2fa;
        }
    }
}

mariadb::AuthenticatorModule* MariaDBClientConnection::find_auth_module(const string& plugin_name)
{
    mariadb::AuthenticatorModule* rval = nullptr;
    auto& auth_modules = m_session->listener_data()->m_authenticators;
    for (const auto& auth_module : auth_modules)
    {
        auto protocol_auth = static_cast<mariadb::AuthenticatorModule*>(auth_module.get());
        if (protocol_auth->supported_plugins().count(plugin_name))
        {
            // Found correct authenticator for the user entry.
            rval = protocol_auth;
            break;
        }
    }
    return rval;
}

/**
 * Read protocol packet and update packet sequence.
 */
std::tuple<bool, GWBUF> MariaDBClientConnection::read_protocol_packet()
{
    auto rval = mariadb::read_protocol_packet(m_dcb);
    auto& [read_ok, buffer] = rval;
    if (!buffer.empty())
    {
        uint8_t seq = MYSQL_GET_PACKET_NO(buffer.data());
        m_sequence = seq;
        m_next_sequence = seq + 1;
    }
    return rval;
}

mariadb::AuthenticationData& MariaDBClientConnection::authentication_data(AuthType type)
{
    return (type == AuthType::NORMAL_AUTH) ? *m_session_data->auth_data : *m_change_user.auth_data;
}

MariaDBClientConnection::StateMachineRes MariaDBClientConnection::read_proxy_header(bool ssl_on)
{
    namespace proxy = mxb::proxy_protocol;
    using Type = proxy::PreParseResult::Type;

    bool read_ok;
    GWBUF buffer;
    if (ssl_on)
    {
        // Must be compatible with SSLRequest packet. See read_ssl_request() for more.
        std::tie(read_ok, buffer) = m_dcb->read_strict(MYSQL_HEADER_LEN, SSL_REQUEST_PACKET_SIZE);
    }
    else
    {
        std::tie(read_ok, buffer) = m_dcb->read(MYSQL_HEADER_LEN, NORMAL_HS_RESP_MAX_SIZE);
    }

    auto rval = StateMachineRes::ERROR;
    if (!buffer.empty())
    {
        // Have at least 4 bytes. This is enough to check if the packet looks like a proxy protocol header.
        if (proxy::packet_hdr_maybe_proxy(buffer.data()))
        {
            if (proxy::is_proxy_protocol_allowed(m_dcb->ip(), m_session->listener_data()->m_proxy_networks))
            {
                bool socket_alive = true;
                if (ssl_on)
                {
                    // In ssl-mode, the entire proxy header may not have been read out yet due to the low
                    // read limit. Attempt to read more. Since this requires io-ops, the connection may fail.
                    socket_alive = read_proxy_hdr_ssl_safe(buffer);
                }

                if (socket_alive)
                {
                    auto send_hdr_error = [this]() {
                        send_mysql_err_packet(1105, "HY000", "Failed to parse proxy header");
                    };

                    auto pre_parse = proxy::pre_parse_header(buffer.data(), buffer.length());
                    if (pre_parse.type == Type::TEXT || pre_parse.type == Type::BINARY)
                    {
                        // Have the entire header, parse it fully.
                        auto parse_res = (pre_parse.type == Type::TEXT) ?
                            proxy::parse_text_header((const char*)buffer.data(), pre_parse.len) :
                            proxy::parse_binary_header(buffer.data());
                        if (parse_res.success)
                        {
                            rval = StateMachineRes::DONE;
                            buffer.consume(pre_parse.len);
                            m_dcb->unread(std::move(buffer));

                            // If client sent "PROXY UNKNOWN" then nothing needs to be done.
                            if (parse_res.is_proxy)
                            {
                                string text_addr_copy = parse_res.peer_addr_str;
                                m_dcb->set_remote_ip_port(parse_res.peer_addr,
                                                          std::move(parse_res.peer_addr_str));
                                m_session->set_host(std::move(text_addr_copy));
                            }
                        }
                        else
                        {
                            send_hdr_error();
                        }
                    }
                    else if (pre_parse.type == Type::INCOMPLETE)
                    {
                        rval = StateMachineRes::IN_PROGRESS;
                        m_dcb->unread(std::move(buffer));
                    }
                    else
                    {
                        send_hdr_error();
                    }
                }
            }
            else
            {
                // Server sends the following error.
                string msg = mxb::string_printf("Proxy header is not accepted from %s",
                                                m_dcb->remote().c_str());
                send_mysql_err_packet(1130, "HY000", msg.c_str());
            }
        }
        else
        {
            // Normal client response. Put it back to the dcb.
            m_dcb->unread(std::move(buffer));
            rval = StateMachineRes::DONE;
        }
    }
    else if (read_ok)
    {
        // Not enough was read to figure out anything. Wait for more data.
        rval = StateMachineRes::IN_PROGRESS;
    }

    return rval;
}

/**
 * Try to read the entire proxy header out of the socket without reading ssl-data. The caller should check
 * the data for completeness afterwards, as the entire header may not have been available yet.
 *
 * @param buffer Buffer for read data.
 * @return False on IO error. True otherwise.
 */
bool MariaDBClientConnection::read_proxy_hdr_ssl_safe(GWBUF& buffer)
{
    /*
     * The length of the proxy header may be unknown and also have to be careful not to read out any
     * SSL data. May need to read multiple times.
     */

    // Helper function for reading more data.
    auto read_more = [this, &buffer](size_t readlen) {
        // Cannot read more than 36 bytes, as the client may have sent SSLRequest and SSL
        // data after proxy header. Known binary header length overrides this limit.
        auto read_limit = std::max((size_t)SSL_REQUEST_PACKET_SIZE, readlen);
        auto [read_ok, temp_buf] = m_dcb->read_strict(0, read_limit);

        int rval = -1;
        if (!temp_buf.empty())
        {
            buffer.merge_back(std::move(temp_buf));
            rval = 1;
        }
        else if (read_ok)
        {
            rval = 0;
        }
        return rval;
    };

    auto rval = true;
    auto INCOMPLETE = mxb::proxy_protocol::PreParseResult::Type::INCOMPLETE;
    mxb::proxy_protocol::PreParseResult header_res;

    do
    {
        header_res = mxb::proxy_protocol::pre_parse_header(buffer.data(), buffer.length());
        if (header_res.type == INCOMPLETE)
        {
            // Try to read some more data, then check again. Binary headers have length info.
            size_t read_limit = 0;
            if (header_res.len > 0)
            {
                mxb_assert(header_res.len > (int)buffer.length());
                read_limit = header_res.len - buffer.length();
            }

            auto read_res = read_more(read_limit);
            if (read_res == 0)
            {
                // Inconclusive, try again later.
                break;
            }
            else if (read_res < 0)
            {
                // Read failed. Do not send error message.
                rval = false;
                break;
            }
        }
    }
    while (header_res.type == INCOMPLETE);
    return rval;
}

void MariaDBClientConnection::deliver_backend_auth_result(GWBUF&& auth_reply)
{
    mxb_assert(m_auth_state == AuthState::WAIT_FOR_BACKEND);
    mxb_assert(!auth_reply.empty());
    auto cmd = mariadb::get_command(auth_reply);
    if (cmd == MYSQL_REPLY_OK)
    {
        m_pt_be_auth_res = PtAuthResult::OK;
    }
    else
    {
        // Send the original error from backend to client, fix sequence. Router may have already called
        // session->kill() so the client-side state machine is not guaranteed to be invoked.
        *(auth_reply.data() + MYSQL_SEQ_OFFSET) = m_next_sequence;
        write(std::move(auth_reply));
        mxs::Listener::mark_auth_as_failed(m_dcb->remote());
        m_session->service->stats().add_failed_auth();
        m_pt_be_auth_res = PtAuthResult::ERROR;
    }

    m_dcb->trigger_read_event();
}

void MariaDBClientConnection::schedule_reverse_name_lookup()
{
    // Need hostname resolution. Run it in the common threadpool.
    auto fetch_nameinfo = [ses_id = m_session->id(), orig_worker = m_session->worker(),
                           user = m_session_data->auth_data->user, client_addr = m_dcb->ip()]() {
        mxb::StopWatch timer;
        auto [lu_success, lu_res] = mxb::reverse_name_lookup(&client_addr);
        auto time_elapsed = timer.split();
        bool too_long = time_elapsed > 1s;
        if (too_long || !lu_success)
        {
            // Lookup failed and/or took a while. Log a warning.
            string addr_str = mxb::ntop((const sockaddr*)&client_addr);
            auto seconds = mxb::to_secs(time_elapsed);
            const char can_be_prevented[] = "The resolution can be prevented either by removing text-form "
                                            "hostname accounts (e.g. user@hostname.com) for that user or by "
                                            "enabling 'skip_name_resolve' in MaxScale settings.";
            if (lu_success)
            {
                MXB_WARNING("Reverse name lookup of address '%s' of incoming client '%s' succeeded but "
                            "took %.1f seconds. %s",
                            addr_str.c_str(), user.c_str(), seconds, can_be_prevented);
            }
            else if (too_long)
            {
                MXB_WARNING("Reverse name lookup of address '%s' of incoming client '%s' failed: %s The "
                            "operation took %.1f seconds. %s",
                            addr_str.c_str(), user.c_str(), lu_res.c_str(), seconds,
                            can_be_prevented);
            }
            else
            {
                MXB_WARNING("Reverse name lookup of address '%s' of incoming client '%s' failed: %s",
                            addr_str.c_str(), user.c_str(), lu_res.c_str());
            }
        }
        else
        {
            MXB_INFO("Client '%s' address '%s' resolved to '%s'.",
                     user.c_str(), mxb::ntop((const sockaddr*)&client_addr).c_str(), lu_res.c_str());
        }

        string resolved_host = lu_success ? std::move(lu_res) : "";
        auto continue_auth = [ses_id, orig_worker, host = std::move(resolved_host)]() {
            mxb_assert(mxs::RoutingWorker::get_current() == orig_worker);
            // The session could have died while name resolution was running.
            auto ses = orig_worker->session_registry().lookup(ses_id);
            if (ses)
            {
                auto mariadb_ses = static_cast<MYSQL_session*>(ses->protocol_data());
                mariadb_ses->host = host;
                // Send fake read event to get the connection state machine running.
                ses->client_dcb->trigger_read_event();
            }
        };
        // Send the result to the original worker.
        orig_worker->execute(continue_auth, mxs::RoutingWorker::EXECUTE_QUEUED);
    };
    mxs::thread_pool().execute(fetch_nameinfo, "rdns");
}
