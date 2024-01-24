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

#include "internal/server.hh"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>

#include <mutex>
#include <string>
#include <vector>

#include <maxbase/format.hh>
#include <maxbase/log.hh>
#include <maxbase/stopwatch.hh>

#include <maxscale/config2.hh>
#include <maxscale/dcb.hh>
#include <maxscale/http.hh>
#include <maxscale/json_api.hh>
#include <maxscale/parser.hh>
#include <maxscale/routingworker.hh>
#include <maxscale/session.hh>
#include <maxscale/ssl.hh>
#include <maxscale/threadpool.hh>
#include <maxscale/utils.hh>

#include "internal/config.hh"
#include "internal/monitormanager.hh"
#include "internal/servermanager.hh"
#include "internal/session.hh"

using maxbase::Worker;
using maxscale::RoutingWorker;

using std::string;
using Guard = std::lock_guard<std::mutex>;
using namespace std::literals::chrono_literals;
using namespace std::literals::string_literals;

namespace cfg = mxs::config;
using Operation = mxb::MeasureTime::Operation;

namespace
{

constexpr const char CN_EXTRA_PORT[] = "extra_port";
constexpr const char CN_MONITORPW[] = "monitorpw";
constexpr const char CN_MONITORUSER[] = "monitoruser";
constexpr const char CN_PRIORITY[] = "priority";

const char ERR_TOO_LONG_CONFIG_VALUE[] = "The new value for %s is too long. Maximum length is %i characters.";

/**
 * Write to char array by first zeroing any extra space. This reduces effects of concurrent reading.
 * Concurrent writing should be prevented by the caller.
 *
 * @param dest Destination buffer. The buffer is assumed to contains at least a \0 at the end.
 * @param max_len Size of destination buffer - 1. The last element (max_len) is never written to.
 * @param source Source string. A maximum of @c max_len characters are copied.
 */
void careful_strcpy(char* dest, size_t max_len, const std::string& source)
{
    // The string may be accessed while we are updating it.
    // Take some precautions to ensure that the string cannot be completely garbled at any point.
    // Strictly speaking, this is not fool-proof as writes may not appear in order to the reader.
    size_t new_len = source.length();
    if (new_len > max_len)
    {
        new_len = max_len;
    }

    size_t old_len = strlen(dest);
    if (new_len < old_len)
    {
        // If the new string is shorter, zero out the excess data.
        memset(dest + new_len, 0, old_len - new_len);
    }

    // No null-byte needs to be set. The array starts out as all zeros and the above memset adds
    // the necessary null, should the new string be shorter than the old.
    strncpy(dest, source.c_str(), new_len);
}

class ServerSpec : public cfg::Specification
{
public:
    using cfg::Specification::Specification;

protected:

    template<class Params>
    bool do_post_validate(Params& params) const;

    bool post_validate(const cfg::Configuration* config,
                       const mxs::ConfigParameters& params,
                       const std::map<std::string, mxs::ConfigParameters>& nested_params) const override
    {
        return do_post_validate(params);
    }

    bool post_validate(const cfg::Configuration* config,
                       json_t* json,
                       const std::map<std::string, json_t*>& nested_params) const override
    {
        return do_post_validate(json);
    }
};

static const auto NO_QUOTES = cfg::ParamString::IGNORED;
static const auto AT_RUNTIME = cfg::Param::AT_RUNTIME;

static ServerSpec s_spec(CN_SERVERS, cfg::Specification::SERVER);

static cfg::ParamString s_type(&s_spec, CN_TYPE, "Object type", "server", NO_QUOTES);
static cfg::ParamString s_protocol(&s_spec, CN_PROTOCOL, "Server protocol (deprecated)", "", NO_QUOTES);
static cfg::ParamString s_authenticator(
    &s_spec, CN_AUTHENTICATOR, "Server authenticator (deprecated)", "", NO_QUOTES);

static cfg::ParamString s_address(&s_spec, CN_ADDRESS, "Server address", "", NO_QUOTES, AT_RUNTIME);
cfg::ParamString s_private_address(&s_spec, "private_address", "Server private address (replication)", "",
                                   cfg::Param::AT_RUNTIME);
static cfg::ParamString s_socket(&s_spec, CN_SOCKET, "Server UNIX socket", "", NO_QUOTES, AT_RUNTIME);
static cfg::ParamCount s_port(&s_spec, CN_PORT, "Server port", 3306, AT_RUNTIME);
static cfg::ParamCount s_extra_port(&s_spec, CN_EXTRA_PORT, "Server extra port", 0, AT_RUNTIME);
static cfg::ParamInteger s_priority(&s_spec, CN_PRIORITY, "Server priority", 0, AT_RUNTIME);
static cfg::ParamString s_monitoruser(&s_spec, CN_MONITORUSER, "Monitor user", "", NO_QUOTES, AT_RUNTIME);
static cfg::ParamPassword s_monitorpw(&s_spec, CN_MONITORPW, "Monitor password", "", NO_QUOTES, AT_RUNTIME);
static cfg::ParamReplOpts s_replication_custom_opts(&s_spec, "replication_custom_options",
                                                    "Custom CHANGE MASTER TO options", AT_RUNTIME);
static cfg::ParamCount s_persistpoolmax(
    &s_spec, CN_PERSISTPOOLMAX, "Maximum size of the persistent connection pool", 0, AT_RUNTIME);

static cfg::ParamSeconds s_persistmaxtime(
    &s_spec, CN_PERSISTMAXTIME, "Maximum time that a connection can be in the pool",
    0s, AT_RUNTIME);

static cfg::ParamBool s_proxy_protocol(
    &s_spec, CN_PROXY_PROTOCOL, "Enable proxy protocol", false, AT_RUNTIME);

static Server::ParamDiskSpaceLimits s_disk_space_threshold(
    &s_spec, CN_DISK_SPACE_THRESHOLD, "Server disk space threshold");

static cfg::ParamEnum<int64_t> s_rank(
    &s_spec, CN_RANK, "Server rank",
    {
        {RANK_PRIMARY, "primary"},
        {RANK_SECONDARY, "secondary"}
    }, RANK_PRIMARY, AT_RUNTIME);

static cfg::ParamCount s_max_routing_connections(
    &s_spec, CN_MAX_ROUTING_CONNECTIONS, "Maximum routing connections", 0, AT_RUNTIME);

//
// TLS parameters
//

static cfg::ParamBool s_ssl(&s_spec, CN_SSL, "Enable TLS for server", false, AT_RUNTIME);

static cfg::ParamPath s_ssl_cert(
    &s_spec, CN_SSL_CERT, "TLS public certificate", cfg::ParamPath::R, "", AT_RUNTIME);
static cfg::ParamPath s_ssl_key(
    &s_spec, CN_SSL_KEY, "TLS private key", cfg::ParamPath::R, "", AT_RUNTIME);
static cfg::ParamPath s_ssl_ca(
    &s_spec, CN_SSL_CA, "TLS certificate authority", cfg::ParamPath::R, "", AT_RUNTIME);

// Alias ssl_ca_cert -> ssl_ca.
static cfg::ParamDeprecated<cfg::ParamAlias> ss_ssl_ca_cert(&s_spec, CN_SSL_CA_CERT, &s_ssl_ca);

static cfg::ParamEnum<mxb::ssl_version::Version> s_ssl_version(
    &s_spec, CN_SSL_VERSION, "Minimum TLS protocol version",
    {
        {mxb::ssl_version::SSL_TLS_MAX, "MAX"},
        {mxb::ssl_version::TLS10, "TLSv10"},
        {mxb::ssl_version::TLS11, "TLSv11"},
        {mxb::ssl_version::TLS12, "TLSv12"},
        {mxb::ssl_version::TLS13, "TLSv13"}
    }, mxb::ssl_version::SSL_TLS_MAX, AT_RUNTIME);

static cfg::ParamString s_ssl_cipher(&s_spec, CN_SSL_CIPHER, "TLS cipher list", "", NO_QUOTES, AT_RUNTIME);

static cfg::ParamCount s_ssl_cert_verify_depth(
    &s_spec, CN_SSL_CERT_VERIFY_DEPTH, "TLS certificate verification depth", 9, AT_RUNTIME);

static cfg::ParamBool s_ssl_verify_peer_certificate(
    &s_spec, CN_SSL_VERIFY_PEER_CERTIFICATE, "Verify TLS peer certificate", false, AT_RUNTIME);

static cfg::ParamBool s_ssl_verify_peer_host(
    &s_spec, CN_SSL_VERIFY_PEER_HOST, "Verify TLS peer host", false, AT_RUNTIME);

template<class Params>
bool ServerSpec::do_post_validate(Params& params) const
{
    bool rval = true;
    auto monuser = s_monitoruser.get(params);
    auto monpw = s_monitorpw.get(params);

    if (monuser.empty() != monpw.empty())
    {
        MXB_ERROR("If '%s is defined, '%s' must also be defined.",
                  !monuser.empty() ? CN_MONITORUSER : CN_MONITORPW,
                  !monuser.empty() ? CN_MONITORPW : CN_MONITORUSER);
        rval = false;
    }

    if (monuser.length() > Server::MAX_MONUSER_LEN)
    {
        MXB_ERROR(ERR_TOO_LONG_CONFIG_VALUE, CN_MONITORUSER, Server::MAX_MONUSER_LEN);
        rval = false;
    }

    if (monpw.length() > Server::MAX_MONPW_LEN)
    {
        MXB_ERROR(ERR_TOO_LONG_CONFIG_VALUE, CN_MONITORPW, Server::MAX_MONPW_LEN);
        rval = false;
    }

    auto address = s_address.get(params);
    auto socket = s_socket.get(params);
    bool have_address = !address.empty();
    bool have_socket = !socket.empty();
    auto addr = have_address ? address : socket;

    if (have_socket && have_address)
    {
        MXB_ERROR("Both '%s=%s' and '%s=%s' defined: only one of the parameters can be defined",
                  CN_ADDRESS, address.c_str(), CN_SOCKET, socket.c_str());
        rval = false;
    }
    else if (!have_address && !have_socket)
    {
        MXB_ERROR("Missing a required parameter: either '%s' or '%s' must be defined",
                  CN_ADDRESS, CN_SOCKET);
        rval = false;
    }
    else if (have_address && addr[0] == '/')
    {
        MXB_ERROR("The '%s' parameter is not a valid IP or hostname", CN_ADDRESS);
        rval = false;
    }
    else if (addr.length() > Server::MAX_ADDRESS_LEN)
    {
        MXB_ERROR(ERR_TOO_LONG_CONFIG_VALUE, have_address ? CN_ADDRESS : CN_SOCKET, Server::MAX_ADDRESS_LEN);
        rval = false;
    }

    if (s_ssl.get(params) && s_ssl_cert.get(params).empty() != s_ssl_key.get(params).empty())
    {
        MXB_ERROR("Both '%s' and '%s' must be defined", s_ssl_cert.name().c_str(), s_ssl_key.name().c_str());
        rval = false;
    }

    return rval;
}

std::pair<bool, std::unique_ptr<mxs::SSLContext>> create_ssl(const char* name, const mxb::SSLConfig& config)
{
    bool ok = true;
    auto ssl = mxs::SSLContext::create(config);

    if (!ssl)
    {
        MXB_ERROR("Unable to initialize SSL for server '%s'", name);
        ok = false;
    }
    else if (!ssl->valid())
    {
        // An empty ssl config should result in an empty pointer. This can be removed if Server stores
        // SSLContext as value.
        ssl.reset();
    }
    else
    {
        ssl->set_usage(mxb::KeyUsage::CLIENT);
    }

    return {ok, std::move(ssl)};
}

void persistpoolmax_modified(const std::string& srvname, int64_t pool_size)
{
    auto func = [=]() {
            RoutingWorker::pool_set_size(srvname, pool_size);
        };
    mxs::RoutingWorker::broadcast(func, nullptr, mxb::Worker::EXECUTE_AUTO);
}

bool is_ip(const std::string& addr)
{
    return mxb::Host::is_valid_ipv4(addr) || mxb::Host::is_valid_ipv6(addr);
}
}

Server::ParamDiskSpaceLimits::ParamDiskSpaceLimits(cfg::Specification* pSpecification,
                                                   const char* zName, const char* zDescription)
    : cfg::ConcreteParam<ParamDiskSpaceLimits, DiskSpaceLimits>(
        pSpecification, zName, zDescription, AT_RUNTIME, OPTIONAL, value_type())
{
}

std::string Server::ParamDiskSpaceLimits::type() const
{
    return "disk_space_limits";
}

std::string Server::ParamDiskSpaceLimits::to_string(Server::ParamDiskSpaceLimits::value_type value) const
{
    std::vector<std::string> tmp;
    std::transform(value.begin(), value.end(), std::back_inserter(tmp),
                   [](const auto& a) {
                       return a.first + ':' + std::to_string(a.second);
                   });
    return mxb::join(tmp, ",");
}

bool Server::ParamDiskSpaceLimits::from_string(const std::string& value, value_type* pValue,
                                               std::string* pMessage) const
{
    return config_parse_disk_space_threshold(pValue, value.c_str());
}

json_t* Server::ParamDiskSpaceLimits::to_json(value_type value) const
{
    json_t* obj = value.empty() ? json_null() : json_object();

    for (const auto& a : value)
    {
        json_object_set_new(obj, a.first.c_str(), json_integer(a.second));
    }

    return obj;
}

bool Server::ParamDiskSpaceLimits::from_json(const json_t* pJson, value_type* pValue,
                                             std::string* pMessage) const
{
    bool ok = false;

    if (json_is_object(pJson))
    {
        ok = true;
        const char* key;
        json_t* value;
        value_type newval;

        json_object_foreach(const_cast<json_t*>(pJson), key, value)
        {
            if (json_is_integer(value))
            {
                newval[key] = json_integer_value(value);
            }
            else
            {
                ok = false;
                *pMessage = "'"s + key + "' is not a JSON number.";
                break;
            }
        }
    }
    else if (json_is_string(pJson))
    {
        // Allow conversion from the INI format string to make it easier to configure this via maxctrl:
        // defining JSON objects with it is not very convenient.
        ok = from_string(json_string_value(pJson), pValue, pMessage);
    }
    else if (json_is_null(pJson))
    {
        ok = true;
    }
    else
    {
        *pMessage = "Not a JSON object or JSON null.";
    }

    return ok;
}

Server::Settings::Settings(const std::string& name, Server* server)
    : mxs::config::Configuration(name, &s_spec)
    , m_type(this, &s_type)
    , m_protocol(this, &s_protocol)
    , m_authenticator(this, &s_authenticator)
    , m_address(this, &s_address)
    , m_private_address(this, &s_private_address)
    , m_socket(this, &s_socket)
    , m_port(this, &s_port)
    , m_extra_port(this, &s_extra_port)
    , m_priority(this, &s_priority)
    , m_monitoruser(this, &s_monitoruser)
    , m_monitorpw(this, &s_monitorpw)
    , m_replication_custom_opts(this, &s_replication_custom_opts)
    , m_persistmaxtime(this, &s_persistmaxtime)
    , m_proxy_protocol(this, &s_proxy_protocol)
    , m_disk_space_threshold(this, &s_disk_space_threshold)
    , m_rank(this, &s_rank)
    , m_max_routing_connections(this, &s_max_routing_connections)
    , m_ssl(this, &s_ssl)
    , m_ssl_cert(this, &s_ssl_cert)
    , m_ssl_key(this, &s_ssl_key)
    , m_ssl_ca(this, &s_ssl_ca)
    , m_ssl_version(this, &s_ssl_version)
    , m_ssl_cert_verify_depth(this, &s_ssl_cert_verify_depth)
    , m_ssl_verify_peer_certificate(this, &s_ssl_verify_peer_certificate)
    , m_ssl_verify_peer_host(this, &s_ssl_verify_peer_host)
    , m_ssl_cipher(this, &s_ssl_cipher)
    , m_persistpoolmax(this, &s_persistpoolmax)
    , m_server(*server)
{
}

bool Server::Settings::post_configure(const std::map<string, mxs::ConfigParameters>& nested)
{
    mxb_assert(nested.empty());

    const string& addr = !m_address.get().empty() ? m_address.get() : m_socket.get();

    careful_strcpy(address, MAX_ADDRESS_LEN, addr);
    careful_strcpy(private_address, MAX_ADDRESS_LEN, m_private_address.get());
    careful_strcpy(monuser, MAX_MONUSER_LEN, m_monitoruser.get());
    careful_strcpy(monpw, MAX_MONPW_LEN, m_monitorpw.get());

    m_have_disk_space_limits.store(!m_disk_space_threshold.get().empty());

    auto persistpoolmax_eff_old = m_persistpoolmax_eff;
    m_persistpoolmax_eff = m_persistpoolmax.get();
    if (m_persistpoolmax_eff > 0)
    {
        auto n_threads = mxs::Config::get().n_threads;
        auto remainder = m_persistpoolmax_eff % n_threads;
        if (remainder != 0)
        {
            m_persistpoolmax_eff += n_threads - remainder;
            MXB_NOTICE("'%s' set to %li to ensure equal poolsize for every thread.",
                       CN_PERSISTPOOLMAX, m_persistpoolmax_eff);
        }
    }

    if (m_persistpoolmax_eff != persistpoolmax_eff_old)
    {
        auto func = [this, srvname = name()]() {
                RoutingWorker::pool_set_size(srvname, m_persistpoolmax_eff);
            };
        mxs::RoutingWorker::broadcast(func, nullptr, mxb::Worker::EXECUTE_AUTO);
    }

    return m_server.post_configure();
}

// static
const cfg::Specification& Server::specification()
{
    return s_spec;
}

std::unique_ptr<Server> Server::create(const char* name, const mxs::ConfigParameters& params)
{
    std::unique_ptr<Server> rval;

    if (s_spec.validate(params))
    {
        if (auto server = std::make_unique<Server>(name))
        {
            if (server->configuration().configure(params))
            {
                rval = std::move(server);
            }
        }
    }

    return rval;
}

std::unique_ptr<Server> Server::create(const char* name, json_t* json)
{
    std::unique_ptr<Server> rval;

    if (s_spec.validate(json))
    {
        if (auto server = std::make_unique<Server>(name))
        {
            if (server->configuration().configure(json))
            {
                rval = std::move(server);
            }
        }
    }

    return rval;
}

Server* Server::create_test_server()
{
    static int next_id = 1;
    string name = "TestServer" + std::to_string(next_id++);
    return new Server(name);
}

void Server::set_status(uint64_t bit)
{
    m_status |= bit;
}

void Server::clear_status(uint64_t bit)
{
    m_status &= ~bit;
}

void Server::assign_status(uint64_t status)
{
    m_status = status;
}

bool Server::set_monitor_user(const string& username)
{
    bool rval = false;
    if (username.length() <= MAX_MONUSER_LEN)
    {
        careful_strcpy(m_settings.monuser, MAX_MONUSER_LEN, username);
        rval = true;
    }
    else
    {
        MXB_ERROR(ERR_TOO_LONG_CONFIG_VALUE, CN_MONITORUSER, MAX_MONUSER_LEN);
    }
    return rval;
}

bool Server::set_monitor_password(const string& password)
{
    bool rval = false;
    if (password.length() <= MAX_MONPW_LEN)
    {
        careful_strcpy(m_settings.monpw, MAX_MONPW_LEN, password);
        rval = true;
    }
    else
    {
        MXB_ERROR(ERR_TOO_LONG_CONFIG_VALUE, CN_MONITORPW, MAX_MONPW_LEN);
    }
    return rval;
}

string Server::monitor_user() const
{
    return m_settings.monuser;
}

string Server::monitor_password() const
{
    return m_settings.monpw;
}

string Server::replication_custom_opts() const
{
    return m_settings.m_replication_custom_opts.get();
}

bool Server::set_address(const string& new_address)
{
    bool rval = false;
    if (new_address.length() <= MAX_ADDRESS_LEN)
    {
        if (m_settings.m_address.set(new_address))
        {
            careful_strcpy(m_settings.address, MAX_ADDRESS_LEN, new_address);
            rval = true;
        }
        else
        {
            MXB_ERROR("The specifed server address '%s' is not valid.", new_address.c_str());
        }
    }
    else
    {
        MXB_ERROR(ERR_TOO_LONG_CONFIG_VALUE, CN_ADDRESS, MAX_ADDRESS_LEN);
    }
    return rval;
}

void Server::set_port(int new_port)
{
    m_settings.m_port.set(new_port);
}

void Server::set_extra_port(int new_port)
{
    m_settings.m_extra_port.set(new_port);
}

std::shared_ptr<mxs::SSLContext> Server::ssl() const
{
    return *m_ssl_ctx;
}

mxb::SSLConfig Server::ssl_config() const
{
    std::lock_guard<std::mutex> guard(m_ssl_lock);
    return m_ssl_config;
}

bool Server::proxy_protocol() const
{
    return m_settings.m_proxy_protocol.get();
}

void Server::set_proxy_protocol(bool proxy_protocol)
{
    m_settings.m_proxy_protocol.set(proxy_protocol);
}

uint8_t Server::charset() const
{
    return m_charset;
}

void Server::set_charset(uint8_t charset)
{
    m_charset = charset;
}

bool Server::set_variables(Variables&& variables)
{
    std::lock_guard<std::mutex> guard(m_var_lock);
    bool changed = m_variables != variables;
    m_variables = std::move(variables);
    return changed;
}

std::map<int, mxs::Collation> Server::collations() const
{
    std::lock_guard guard(m_var_lock);
    return m_collations;
}

void Server::set_collations(std::map<int, mxs::Collation> collations)
{
    std::lock_guard guard(m_var_lock);
    m_collations = std::move(collations);
}

void Server::set_uptime(int64_t uptime)
{
    m_uptime.store(uptime, std::memory_order_relaxed);
}

int64_t Server::get_uptime() const
{
    return m_uptime.load(std::memory_order_relaxed);
}

bool Server::track_variable(std::string_view variable)
{
    std::lock_guard<std::mutex> guard(m_var_lock);
    auto p = m_tracked_variables.emplace(variable);
    return p.second;
}

bool Server::untrack_variable(std::string_view variable)
{
    bool found = false;
    std::lock_guard<std::mutex> guard(m_var_lock);

    if (auto it = m_tracked_variables.find(variable); it != m_tracked_variables.end())
    {
        m_tracked_variables.erase(it);
        found = true;
    }

    return found;
}

Server::TrackedVariables Server::tracked_variables() const
{
    std::lock_guard<std::mutex> guard(m_var_lock);
    return m_tracked_variables;
}

Server::Variables Server::get_variables() const
{
    std::lock_guard<std::mutex> guard(m_var_lock);
    return m_variables;
}

std::string Server::get_variable_value(std::string_view variable) const
{
    std::lock_guard<std::mutex> guard(m_var_lock);

    auto it = m_variables.find(variable);

    return it != m_variables.end() ? it->second : "";
}

uint64_t Server::status_from_string(const char* str)
{
    static std::vector<std::pair<const char*, uint64_t>> status_bits =
    {
        {"running",      SERVER_RUNNING },
        {"master",       SERVER_MASTER  },
        {"slave",        SERVER_SLAVE   },
        {"synced",       SERVER_JOINED  },
        {"maintenance",  SERVER_MAINT   },
        {"maint",        SERVER_MAINT   },
        {"drain",        SERVER_DRAINING},
        {"blr",          SERVER_BLR     },
        {"binlogrouter", SERVER_BLR     }
    };

    for (const auto& a : status_bits)
    {
        if (strcasecmp(str, a.first) == 0)
        {
            return a.second;
        }
    }

    return 0;
}

void Server::set_gtid_list(const std::vector<std::pair<uint32_t, uint64_t>>& domains)
{
    auto fn = [this, domains]() {
        auto& gtids = *m_gtids;

        for (const auto& p : domains)
        {
            gtids[p.first] = p.second;
        }
    };

    mxs::RoutingWorker::broadcast(fn, mxb::Worker::EXECUTE_AUTO);
    mxs::MainWorker::get()->execute(fn, mxb::Worker::EXECUTE_AUTO);
}

void Server::clear_gtid_list()
{
    auto fn = [this]() {
        m_gtids->clear();
    };

    mxs::RoutingWorker::broadcast(fn, mxb::Worker::EXECUTE_AUTO);
    mxs::MainWorker::get()->execute(fn, mxb::Worker::EXECUTE_AUTO);
}

std::unordered_map<uint32_t, uint64_t> Server::get_gtid_list() const
{
    return *m_gtids;
}

uint64_t Server::gtid_pos(uint32_t domain) const
{
    const auto& gtids = *m_gtids;
    auto it = gtids.find(domain);
    return it != gtids.end() ? it->second : 0;
}

void Server::set_version(BaseType base_type, uint64_t version_num, const std::string& version_str,
                         uint64_t caps)
{
    bool changed = m_info.set(base_type, version_num, version_str, caps);
    if (changed)
    {
        auto type_string = m_info.type_string();
        auto vrs = m_info.version_num();
        if (m_info.type() == VersionInfo::Type::POSTGRESQL && vrs.major >= 10)
        {
            MXB_NOTICE("%s sent version string '%s'. Detected type: %s, version: %i.%i.",
                       name(), version_str.c_str(), type_string.c_str(), vrs.major, vrs.minor);
        }
        else
        {
            MXB_NOTICE("%s sent version string '%s'. Detected type: %s, version: %i.%i.%i.",
                       name(), version_str.c_str(), type_string.c_str(), vrs.major, vrs.minor, vrs.patch);
        }
    }
}

json_t* Server::json_parameters() const
{
    /** Store server parameters in attributes */
    json_t* params = m_settings.to_json();

    // Return either address/port or socket, not both
    auto socket = json_object_get(params, CN_SOCKET);

    if (socket && !json_is_null(socket))
    {
        mxb_assert(json_is_string(socket));
        json_object_set_new(params, CN_ADDRESS, json_null());
        json_object_set_new(params, CN_PORT, json_null());
    }
    else
    {
        json_object_set_new(params, CN_SOCKET, json_null());
    }

    // Remove unwanted parameters
    json_object_del(params, CN_TYPE);
    json_object_del(params, CN_AUTHENTICATOR);
    json_object_del(params, CN_PROTOCOL);

    return params;
}

json_t* Server::json_attributes() const
{
    /** Resource attributes */
    json_t* attr = json_object();

    json_object_set_new(attr, CN_PARAMETERS, json_parameters());

    /** Store general information about the server state */
    string stat = status_string();
    json_object_set_new(attr, CN_STATE, json_string(stat.c_str()));

    json_object_set_new(attr, CN_VERSION_STRING, json_string(m_info.version_string()));
    json_object_set_new(attr, "replication_lag", json_integer(replication_lag()));
    json_object_set_new(attr, "uptime", json_integer(get_uptime()));

    json_t* statistics = stats().to_json();
    auto pool_stats = mxs::RoutingWorker::pool_get_stats(this);
    json_object_set_new(statistics, "persistent_connections", json_integer(pool_stats.curr_size));
    json_object_set_new(statistics, "max_pool_size", json_integer(pool_stats.max_size));
    json_object_set_new(statistics, "reused_connections", json_integer(pool_stats.times_found));
    json_object_set_new(statistics, "connection_pool_empty", json_integer(pool_stats.times_empty));
    maxbase::Duration response_ave(mxb::from_secs(response_time_average()));
    json_object_set_new(statistics, "adaptive_avg_select_time",
                        json_string(mxb::to_string(response_ave).c_str()));


    if (is_resp_distribution_enabled())
    {
        const auto& distr_obj = json_object();
        json_object_set_new(distr_obj, "read", response_distribution_to_json(Operation::READ));
        json_object_set_new(distr_obj, "write", response_distribution_to_json(Operation::WRITE));
        json_object_set_new(statistics, "response_time_distribution", distr_obj);
    }

    json_object_set_new(attr, "statistics", statistics);

    json_object_set_new(attr, CN_SOURCE, mxs::Config::object_source_to_json(name()));

    // Retrieve additional server-specific attributes from monitor and combine it with the base data.
    if (auto extra = MonitorManager::monitored_server_attributes_json(this))
    {
        json_object_update(attr, extra);
        json_decref(extra);
    }

    return attr;
}

json_t* Server::response_distribution_to_json(Operation opr) const
{
    const auto& distr_obj = json_object();
    const auto& arr = json_array();
    auto my_distribution = get_complete_response_distribution(opr);

    for (const auto& element : my_distribution.get())
    {
        auto row_obj = json_object();

        json_object_set_new(row_obj, "time",
                            json_string(std::to_string(mxb::to_secs(element.limit)).c_str()));
        json_object_set_new(row_obj, "total", json_real(mxb::to_secs(element.total)));
        json_object_set_new(row_obj, "count", json_integer(element.count));

        json_array_append_new(arr, row_obj);
    }
    json_object_set_new(distr_obj, "distribution", arr);
    json_object_set_new(distr_obj, "range_base",
                        json_integer(my_distribution.range_base()));
    json_object_set_new(distr_obj, "operation", json_string(opr == Operation::READ ? "read" : "write"));

    return distr_obj;
}

json_t* Server::to_json_data(const char* host) const
{
    json_t* rval = json_object();

    /** Add resource identifiers */
    json_object_set_new(rval, CN_ID, json_string(name()));
    json_object_set_new(rval, CN_TYPE, json_string(CN_SERVERS));

    /** Attributes */
    json_object_set_new(rval, CN_ATTRIBUTES, json_attributes());
    json_object_set_new(rval, CN_LINKS, mxs_json_self_link(host, CN_SERVERS, name()));

    return rval;
}

bool Server::post_configure()
{
    bool ok;
    std::shared_ptr<mxs::SSLContext> ctx;
    std::tie(ok, ctx) = create_ssl(m_name.c_str(), create_ssl_config());

    if (ok)
    {
        m_ssl_ctx.assign(ctx);
        std::lock_guard<std::mutex> guard(m_ssl_lock);
        m_ssl_config = ctx ? ctx->config() : mxb::SSLConfig();
    }

    return ok;
}

mxb::SSLConfig Server::create_ssl_config()
{
    mxb::SSLConfig cfg;

    cfg.enabled = m_settings.m_ssl.get();
    cfg.key = m_settings.m_ssl_key.get();
    cfg.cert = m_settings.m_ssl_cert.get();
    cfg.ca = m_settings.m_ssl_ca.get();
    cfg.version = m_settings.m_ssl_version.get();
    cfg.verify_peer = m_settings.m_ssl_verify_peer_certificate.get();
    cfg.verify_host = m_settings.m_ssl_verify_peer_host.get();
    cfg.verify_depth = m_settings.m_ssl_cert_verify_depth.get();
    cfg.cipher = m_settings.m_ssl_cipher.get();

    return cfg;
}

bool
Server::VersionInfo::set(BaseType base_type, uint64_t version, const std::string& version_str, uint64_t caps)
{
    uint32_t major = version / 10000;
    uint32_t minor = (version - major * 10000) / 100;
    uint32_t patch = version - major * 10000 - minor * 100;

    Type new_type = Type::UNKNOWN;

    if (base_type == BaseType::MARIADB)
    {
        auto version_strz = version_str.c_str();
        if (strcasestr(version_strz, "xpand") || strcasestr(version_strz, "clustrix"))
        {
            new_type = Type::XPAND;
        }
        else if (strcasestr(version_strz, "binlogrouter"))
        {
            new_type = Type::BLR;
        }
        else if (strcasestr(version_strz, "mariadb"))
        {
            // Needs to be after Xpand and BLR as their version strings may include "mariadb".
            new_type = Type::MARIADB;
        }
        else if (!version_str.empty())
        {
            new_type = Type::MYSQL;     // Used for any unrecognized server types.
        }
    }
    else
    {
        // Pg versions prior to major 10 used three-part versioning similar to MariaDB. After 10, only two
        // parts, major and minor are used.
        if (major >= 10)
        {
            // The minor version is the last digit in this case.
            minor = patch;
            patch = 0;
        }
        new_type = Type::POSTGRESQL;
    }

    bool changed = false;
    /* This only protects against concurrent writing which could result in garbled values. Reads are not
     * synchronized. Since writing is rare, this is an unlikely issue. Readers should be prepared to
     * sometimes get inconsistent values. */
    Guard lock(m_lock);

    if (new_type != m_type || version != m_version_num.total || version_str != m_version_str)
    {
        m_caps = caps;
        m_type = new_type;
        m_version_num.total = version;
        m_version_num.major = major;
        m_version_num.minor = minor;
        m_version_num.patch = patch;
        careful_strcpy(m_version_str, MAX_VERSION_LEN, version_str);
        changed = true;
    }
    return changed;
}

const Server::VersionInfo::Version& Server::VersionInfo::version_num() const
{
    return m_version_num;
}

Server::VersionInfo::Type Server::VersionInfo::type() const
{
    return m_type;
}

const char* Server::VersionInfo::version_string() const
{
    return m_version_str;
}

bool SERVER::VersionInfo::is_database() const
{
    auto t = m_type;
    return t == Type::MARIADB || t == Type::XPAND || t == Type::MYSQL || t == Type::POSTGRESQL;
}

std::string SERVER::VersionInfo::type_string() const
{
    string type_str;
    switch (m_type)
    {
    case Type::UNKNOWN:
        type_str = "Unknown";
        break;

    case Type::MYSQL:
        type_str = "MySQL";
        break;

    case Type::MARIADB:
        type_str = "MariaDB";
        break;

    case Type::XPAND:
        type_str = "Xpand";
        break;

    case Type::BLR:
        type_str = "MaxScale Binlog Router";
        break;

    case Type::POSTGRESQL:
        type_str = "PostgreSQL";
        break;
    }
    return type_str;
}

uint64_t SERVER::VersionInfo::capabilities() const
{
    return m_caps;
}

const SERVER::VersionInfo& Server::info() const
{
    return m_info;
}

maxscale::ResponseDistribution& Server::response_distribution(Operation opr)
{
    mxb_assert(opr != Operation::NOP);

    if (opr == Operation::READ)
    {
        return *m_read_distributions;
    }
    else
    {
        return *m_write_distributions;
    }
}

const maxscale::ResponseDistribution& Server::response_distribution(Operation opr) const
{
    return const_cast<Server*>(this)->response_distribution(opr);
}

// The threads modify a reference to a ResponseDistribution, which is
// in a WorkerGlobal. So when the code below reads a copy (the +=)
// there can be a small inconsistency: the count might have been updated,
// but the total not, or even the other way around as there are no atomics
// in ResponseDistribution.
// Fine, it is still thread safe. All in the name of performance.
maxscale::ResponseDistribution Server::get_complete_response_distribution(Operation opr) const
{
    mxb_assert(opr != Operation::NOP);

    maxscale::ResponseDistribution ret = m_read_distributions->with_stats_reset();

    const auto& distr = (opr == Operation::READ) ? m_read_distributions : m_write_distributions;

    for (auto rhs : distr.collect_values())
    {
        ret += rhs;
    }

    return ret;
}

std::shared_ptr<mxs::Endpoint> Server::get_connection(mxs::Component* up, MXS_SESSION* session)
{
    return std::make_shared<ServerEndpoint>(up, session, this);
}

std::ostream& Server::persist(std::ostream& os) const
{
    return m_settings.persist(os, {s_type.name()});
}


SERVER* SERVER::find_by_unique_name(const string& name)
{
    return ServerManager::find_by_unique_name(name);
}

std::vector<SERVER*> SERVER::server_find_by_unique_names(const std::vector<string>& server_names)
{
    std::vector<SERVER*> rval;
    rval.reserve(server_names.size());
    for (auto elem : server_names)
    {
        rval.push_back(ServerManager::find_by_unique_name(elem));
    }
    return rval;
}

bool Server::is_mxs_service() const
{
    bool rval = false;

    /** Do a coarse check for local server pointing to a MaxScale service */
    if (address()[0] == '/')
    {
        if (service_socket_is_used(address()))
        {
            rval = true;
        }
    }
    else if (strcmp(address(), "127.0.0.1") == 0
             || strcmp(address(), "::1") == 0
             || strcmp(address(), "localhost") == 0
             || strcmp(address(), "localhost.localdomain") == 0)
    {
        if (service_port_is_used(port()))
        {
            rval = true;
        }
    }

    return rval;
}

mxs::ConfigParameters Server::to_params() const
{
    return m_settings.to_params();
}


mxs::config::Configuration& Server::configuration()
{
    return m_settings;
}

/**
 * ServerEndpoint
 */
ServerEndpoint::ServerEndpoint(mxs::Component* up, MXS_SESSION* session, Server* server)
    : m_up(up)
    , m_session(static_cast<Session*>(session))
    , m_server(server)
    , m_query_time(RoutingWorker::get_current())
    , m_read_distribution(server->response_distribution(Operation::READ))
    , m_write_distribution(server->response_distribution(Operation::WRITE))
{
}

ServerEndpoint::~ServerEndpoint()
{
    if (is_open())
    {
        close();
    }
}

void ServerEndpoint::connect()
{
    mxb_assert(m_connstatus == ConnStatus::NO_CONN || m_connstatus == ConnStatus::IDLE_POOLED);
    mxb::LogScope scope(m_server->name());
    auto worker = m_session->worker();
    auto res = worker->get_backend_connection(m_server, m_session, this);

    if (res.conn)
    {
        m_conn = res.conn;
        m_connstatus = ConnStatus::CONNECTED;
    }
    else if (res.conn_limit_reached)
    {
        if (m_session->idle_pooling_enabled())
        {
            // Connection count limit exceeded, but pre-emptive pooling is on. Assume that a
            // connection will soon be available. Add an entry to the worker so that the endpoint can
            // be notified as soon as a connection becomes available.

            m_connstatus = ConnStatus::WAITING_FOR_CONN;
            worker->add_conn_wait_entry(this);
            m_conn_wait_start = worker->epoll_tick_now();

            MXB_INFO("Server '%s' connection count limit reached while pre-emptive pooling is on. "
                     "Delaying query until a connection becomes available.", m_server->name());
        }
        else
        {
            throw mxb::Exception(mxb::string_printf(
                "'%s' connection count limit reached. No new connections can "
                "be made until an existing session quits.", m_server->name()));
        }
    }
    else
    {
        m_connstatus = ConnStatus::NO_CONN;
        throw mxb::Exception("Connection failure");
    }
}

void ServerEndpoint::close()
{
    mxb::LogScope scope(m_server->name());

    bool normal_close = (m_connstatus == ConnStatus::CONNECTED);
    if (normal_close || m_connstatus == ConnStatus::CONNECTED_FAILED)
    {
        auto* dcb = m_conn->dcb();
        bool moved_to_pool = false;
        if (normal_close)
        {
            // Try to move the connection into the pool. If it fails, close normally.
            moved_to_pool = dcb->session()->normal_quit() && dcb->manager()->move_to_conn_pool(dcb);
        }

        if (moved_to_pool)
        {
            mxb_assert(dcb->is_open());
        }
        else
        {
            BackendDCB::close(dcb);
            m_server->stats().remove_connection();
        }
        m_conn = nullptr;
        m_session->worker()->notify_connection_available(m_server);
    }
    else if (m_connstatus == ConnStatus::WAITING_FOR_CONN)
    {
        // Erase the entry in the wait list.
        m_session->worker()->erase_conn_wait_entry(this);
    }

    // This function seems to be called twice when closing an Endpoint. Take this into account by always
    // setting connstatus. Should be fixed properly at some point.
    m_connstatus = ConnStatus::NO_CONN;
}

void ServerEndpoint::handle_failed_continue()
{
    mxs::Reply dummy;
    // Need to give some kind of error packet or handleError will crash. The Endpoint will be closed
    // after the call.
    auto error = "Lost connection to server when reusing connection.";
    m_up->handleError(mxs::ErrorType::PERMANENT, error, this, dummy);
}

void ServerEndpoint::handle_timed_out_continue()
{
    m_connstatus = ConnStatus::NO_CONN;
    mxs::Reply dummy;
    auto error = "Timed out when waiting for a connection.";
    m_up->handleError(mxs::ErrorType::PERMANENT, error, this, dummy);
}

bool ServerEndpoint::is_open() const
{
    return m_connstatus != ConnStatus::NO_CONN;
}

bool ServerEndpoint::routeQuery(GWBUF&& buffer)
{
    mxb::LogScope scope(m_server->name());
    mxb_assert(is_open());
    mxb_assert(buffer);
    MXB_MAYBE_EXCEPTION();
    int32_t rval = 0;
    auto not_master = !(m_server->status() & SERVER_MASTER);
    auto opr = not_master ? Operation::READ : Operation::WRITE;

    if (rcap_type_required(m_session->capabilities(), RCAP_TYPE_QUERY_CLASSIFICATION))
    {
        const uint32_t read_only_types = mxs::sql::TYPE_READ
            | mxs::sql::TYPE_USERVAR_READ | mxs::sql::TYPE_SYSVAR_READ | mxs::sql::TYPE_GSYSVAR_READ;

        uint32_t type_mask = 0;

        auto* parser = m_session->client_connection()->parser();
        // TODO: These could be combined.
        if (parser->is_query(buffer) || parser->is_prepare(buffer))
        {
            type_mask = session()->client_connection()->parser()->get_type_mask(buffer);
        }

        auto is_read_only = !(type_mask & ~read_only_types);
        auto is_read_only_trx = m_session->protocol_data()->is_trx_read_only();
        opr = (not_master || is_read_only || is_read_only_trx) ? Operation::READ : Operation::WRITE;
    }

    switch (m_connstatus)
    {
    case ConnStatus::NO_CONN:
    case ConnStatus::CONNECTED_FAILED:
        mxb_assert(!true);      // Means that an earlier failure was not properly handled.
        break;

    case ConnStatus::CONNECTED:
        rval = m_conn->routeQuery(std::move(buffer));
        m_server->stats().add_packet();
        break;

    case ConnStatus::IDLE_POOLED:
        // Connection was pre-emptively pooled. Try to get another one.
        try
        {
            connect();

            if (m_connstatus == ConnStatus::CONNECTED)
            {
                MXB_INFO("Session %lu connection to %s restored from pool.",
                         m_session->id(), m_server->name());
                rval = m_conn->routeQuery(std::move(buffer));
                m_server->stats().add_packet();
            }
            else
            {
                // Waiting for another one.
                m_delayed_packets.emplace_back(std::move(buffer));
                rval = 1;
            }
        }
        catch (const mxb::Exception& e)
        {
            // Connection failed, return error.
        }
        break;

    case ConnStatus::WAITING_FOR_CONN:
        // Already waiting for a connection. Save incoming buffer so it can be sent once a connection
        // is available.
        m_delayed_packets.emplace_back(std::move(buffer));
        rval = 1;
        break;
    }
    m_query_time.start(opr);    // always measure
    return rval;
}

bool ServerEndpoint::clientReply(GWBUF&& buffer, const mxs::ReplyRoute& down, const mxs::Reply& reply)
{
    mxb::LogScope scope(m_server->name());
    mxb_assert(is_open());
    mxb_assert(buffer);

    m_query_time.stop();    // always measure

    if (m_query_time.opr() == Operation::READ)
    {
        m_read_distribution.add(m_query_time.duration());
    }
    else
    {
        m_write_distribution.add(m_query_time.duration());
    }

    return m_up->clientReply(std::move(buffer), mxs::ReplyRoute {this, &down}, reply);
}

bool ServerEndpoint::handleError(mxs::ErrorType type, const std::string& error,
                                 mxs::Endpoint* down, const mxs::Reply& reply)
{
    mxb::LogScope scope(m_server->name());
    mxb_assert(is_open());
    return m_up->handleError(type, error, this, reply);
}

bool ServerEndpoint::try_to_pool()
{
    bool rval = false;
    if (m_connstatus == ConnStatus::CONNECTED)
    {
        auto* dcb = m_conn->dcb();
        if (dcb->manager()->move_to_conn_pool(dcb))
        {
            rval = true;
            m_connstatus = ConnStatus::IDLE_POOLED;
            m_conn = nullptr;
            m_up->endpointConnReleased(this);
            MXB_INFO("Session %lu connection to %s pooled.", m_session->id(), m_server->name());
            m_session->worker()->notify_connection_available(m_server);
        }
    }
    return rval;
}

ServerEndpoint::ContinueRes ServerEndpoint::continue_connecting()
{
    mxb_assert(m_connstatus == ConnStatus::WAITING_FOR_CONN);
    auto res = m_session->worker()->get_backend_connection(m_server, m_session, this);
    auto rval = ContinueRes::FAIL;
    if (res.conn)
    {
        m_conn = res.conn;
        m_connstatus = ConnStatus::CONNECTED;

        // Send all pending packets one by one to the connection. The physical connection may not be ready
        // yet, but the protocol should keep track of the state.
        bool success = true;
        for (auto& packet : m_delayed_packets)
        {
            if (m_conn->routeQuery(std::move(packet)) == 0)
            {
                success = false;
                break;
            }
        }
        m_delayed_packets.clear();

        if (success)
        {
            rval = ContinueRes::SUCCESS;
        }
        else
        {
            // This special state ensures the connection is not pooled.
            m_connstatus = ConnStatus::CONNECTED_FAILED;
        }
    }
    else if (res.conn_limit_reached)
    {
        // Still no connection.
        rval = ContinueRes::WAIT;
    }
    else
    {
        m_connstatus = ConnStatus::NO_CONN;
    }
    return rval;
}

SERVER* ServerEndpoint::server() const
{
    return m_server;
}

Session* ServerEndpoint::session() const
{
    return m_session;
}

mxb::TimePoint ServerEndpoint::conn_wait_start() const
{
    return m_conn_wait_start;
}

mxs::Component* ServerEndpoint::parent() const
{
    return m_up;
}

const std::set<std::string>& Server::protocols() const
{
    static std::set<std::string> any_protocol{MXS_ANY_PROTOCOL};
    return any_protocol;
}

void Server::set_maintenance()
{
    mxs::MainWorker::get()->execute([this]() {
        MonitorManager::set_server_status(this, SERVER_MAINT);
    }, mxb::Worker::EXECUTE_AUTO);
}

int Server::connect_socket(sockaddr_storage* addr)
{
    int so = -1;
    size_t sz;
    auto host = address();
    if (host[0] == '/')
    {
        so = open_unix_socket(MxsSocketType::CONNECT, (sockaddr_un*)addr, host);
        sz = sizeof(sockaddr_un);
    }
    else
    {
        const SAddrInfo& ai = *m_addr_info;
        if (ai)
        {
            so = open_outbound_network_socket(*ai, port(), addr);
            sz = sizeof(sockaddr_storage);
        }
        else
        {
            // This may happen if the NEED_DNS-state is re-added to the server.
            mxb_assert(!true);
            MXB_ERROR("Server %s hostname %s has not been resolved to a valid address, cannot create "
                      "connection.", name(), host);
        }
    }

    if (so != -1)
    {
        if (::connect(so, (sockaddr*)addr, sz) == -1 && errno != EINPROGRESS)
        {
            MXB_ERROR("Failed to connect backend server %s ([%s]:%d). Error %d: %s.",
                      name(), host, port(), errno, mxb_strerror(errno));
            ::close(so);
            so = -1;
        }
    }
    else
    {
        MXB_ERROR("Establishing connection to backend server %s ([%s]:%d) failed.", name(), host, port());
    }
    return so;
}

void Server::update_addr_info()
{
    // Address can be accessed safely in any thread as Server* is always valid.
    const char* addr = address();
    if (*addr == '/')
    {
        // Should rarely get here. Perhaps possible if refresh was scheduled just before server address
        // was updated.
    }
    else
    {
        bool is_hostname = true;
        int flags = 0;

        if (is_ip(addr))
        {
            is_hostname = false;
            flags = AI_NUMERICHOST;         // This prevents DNS lookups from taking place
        }

        auto [sAi, errmsg] = mxs::getaddrinfo(addr, flags);
        if (sAi)
        {
            // The master value (shared_ptr) should always be non-null but the contained unique_ptr
            // may be null.
            std::shared_ptr<const SAddrInfo> curr_value = m_addr_info.get_master_ref();
            if (!addrinfo_equal(sAi.get(), curr_value->get()))
            {
                // Print new resolved address if it's a hostname.
                if (is_hostname)
                {
                    string resolved_hn = mxb::ntop(sAi.get()->ai_addr);
                    MXB_NOTICE("Server %s hostname '%s' resolved to %s.",
                               name(), addr, resolved_hn.c_str());
                }

                m_addr_info.assign(std::make_shared<SAddrInfo>(std::move(sAi)));

                auto clear_bit = [this]() {
                    MXB_AT_DEBUG(bool ret = ) MonitorManager::clear_server_status_fast(
                        this, SERVER_NEED_DNS);
                    mxb_assert(ret);
                };
                mxs::MainWorker::get()->execute(clear_bit, mxb::Worker::EXECUTE_AUTO);
            }
        }
        else
        {
            // TODO: think if empty result should override a valid result in some cases.
            MXB_ERROR("Failed to obtain address for server %s host %s: %s",
                      name(), addr, errmsg.c_str());
        }
    }
}

void Server::schedule_addr_info_update()
{
    mxs::thread_pool().execute(std::bind(&Server::update_addr_info, this),
                               mxb::string_printf("getaddrinfo %s", address()));
}

void Server::start_addr_info_update()
{
    // The update should only be started by the addition of a server in servermanager.cc or when the address
    // is changed in config_runtime.cc
    mxb_assert(mxs::MainWorker::is_current());

    if (m_addr_update_dcid != mxb::Worker::NO_CALL)
    {
        cancel_dcall(m_addr_update_dcid, false);
        m_addr_update_dcid = mxb::Worker::NO_CALL;
    }

    if (*address() == '/')
    {
        // getaddrinfo does not apply to unix sockets.
        // socket address, clear address info.
        m_addr_info.assign(std::make_shared<SAddrInfo>());
    }
    else if (is_ip(address()))
    {
        // The address is an IP address which means it will never change and it can be resolved immediately
        update_addr_info();
    }
    else
    {
        // Refresh server address info immediately and every minute afterward.
        schedule_addr_info_update();

        m_addr_update_dcid = dcall(60s, [this]() {
            schedule_addr_info_update();
            return true;
        });
    }
}

Server::~Server()
{
    cancel_dcall(m_addr_update_dcid, false);
}
