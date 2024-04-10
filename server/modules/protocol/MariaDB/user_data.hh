/*
 * Copyright (c) 2019 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-02-27
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#pragma once

#include <maxscale/ccdefs.hh>

#include <condition_variable>
#include <map>
#include <optional>
#include <set>
#include <thread>
#include <maxsql/mariadb_connector.hh>
#include <maxsql/queryresult.hh>
#include <maxscale/protocol2.hh>
#include <maxscale/protocol/mariadb/authenticator.hh>
#include <maxscale/protocol/mariadb/protocol_classes.hh>
#include <maxscale/server.hh>

/**
 * This class contains user data retrieved from the mysql-database.
 */
class UserDatabase
{
public:
    // The object can be copied but should not be.
    UserDatabase& operator=(const UserDatabase&) = delete;

    // Using normal maps/sets so that entries can be printed in order.
    using StringSet = std::set<std::string>;
    using StringSetMap = std::map<std::string, StringSet>;
    using DBNameCmpMode = mariadb::UserSearchSettings::DBNameCmpMode;

    /**
     * Add an entry to the user database. The entry is not added if the same username@host-combination
     * already exists in the database.
     *
     * @param entry Entry to add
     * @return True if entry was added
     */
    bool add_entry(mariadb::UserEntry&& entry);

    void add_db_grants(StringSetMap&& db_wc_grants, StringSetMap&& db_grants);
    void add_role_mapping(StringSetMap&& role_mapping);

    void   add_database_name(const std::string& db_name);
    void   clear();
    size_t n_usernames() const;
    size_t n_entries() const;
    bool   empty() const;

    /**
     * Find a user entry with matching user & host.
     *
     * @param username Client username. This must match exactly with the entry.
     * @param ip Client address. This must match the entry host pattern.
     * @return The found entry, or null if not found. The pointer should not be saved, as the
     * contents may go invalid after a refresh.
     */
    const mariadb::UserEntry* find_entry(const std::string& username, const std::string& ip) const;

    /**
     * Find a user entry with matching user. Picks the first entry with a matching username without
     * considering the client address.
     *
     * @param username Client username. This must match exactly with the entry.
     * @return The found entry, or null if not found. The pointer should not be saved, as the
     * contents may go invalid after a refresh.
     */
    const mariadb::UserEntry* find_entry(const std::string& username) const;

    /**
     * Find a user entry with matching user & host pattern.
     *
     * @param username Client username. This must match exactly with the entry.
     * @param host Client address. This must match exactly the entry host pattern.
     * @return The found entry, or null if not found. The pointer should not be saved, as the
     * contents may go invalid after a refresh.
     */
    const mariadb::UserEntry*
    find_entry_equal(const std::string& username, const std::string& host_pattern) const;

    /**
     * Find a mutable entry with exact matching user & host pattern. This should only be used when
     * constructing the UserDatabase. Modifying the contents after data has been made available to routing
     * workers is unsafe. Also, fields which affect ordering in the container should not be modified.
     *
     * @param username Username to find
     * @param host_pattern Host pattern to find. Must match exactly.
     * @return Entry, or null of not found
     */
    mariadb::UserEntry*
    find_mutable_entry_equal(const std::string& username, const std::string& host_pattern);

    bool check_database_exists(const std::string& db, bool case_sensitive_db) const;

    /**
     * Check if user entry can access database. The access may be granted with a direct grant or through
     * the default role.
     *
     * @param entry User entry
     * @param db Target database
     * @param case_sensitive_db If true, database names are compared case sensitive
     * @return True if user can access database
     */
    bool check_database_access(const mariadb::UserEntry& entry, const std::string& db,
                               bool case_sensitive_db) const;

    bool equal_contents(const UserDatabase& rhs) const;

    /**
     * Print contents to json.
     *
     * @service_name Name of owning service
     * @return user, host, etc as json
     */
    json_t* users_to_json() const;

    static std::string form_db_mapping_key(const std::string& user, const std::string& host);

private:
    bool user_can_access_db(const std::string& user, const std::string& host_pattern,
                            const std::string& target_db, bool case_sensitive_db) const;
    bool user_can_access_role(const std::string& user, const std::string& host_pattern,
                              const std::string& target_role) const;
    bool role_can_access_db(const std::string& role, const std::string& db, bool case_sensitive_db) const;

    struct HostnameRes
    {
        bool lookup_error {false};
        std::string hostname;
    };
    bool address_matches_host_pattern(const std::string& addr, HostnameRes& hostname,
                                      const mariadb::UserEntry& entry) const;

    enum class HostPatternMode
    {
        SKIP,
        MATCH,
        EQUAL,
    };

    const mariadb::UserEntry*
    find_entry(const std::string& username, const std::string& ip, HostPatternMode mode) const;

    enum class AddrType
    {
        UNKNOWN,
        IPV4,
        MAPPED,
        IPV6,
        LOCALHOST,      /**< If connecting via socket, the remote address is "localhost" */
    };

    enum class PatternType
    {
        UNKNOWN,
        ADDRESS,
        MASK,
        HOSTNAME,
        WC_ADDR_OR_HN,
    };

    AddrType    parse_address_type(const std::string& addr) const;
    PatternType parse_pattern_type(const std::string& host_pattern) const;

    void update_mapping(StringSetMap& target, StringSetMap&& source);

    using EntryList = std::vector<mariadb::UserEntry>;

    /**
     * Map of username -> EntryList. In the list, entries are ordered from most specific hostname pattern to
     * least specific. In effect, contains data from mysql.user-table.
     */
    std::map<std::string, EntryList> m_users;

    /** Maps "user@host" to allowed databases. Retrieved from mysql.db. The database names may contain
     * wildcard characters _ and %, and should be matched accordingly. */
    StringSetMap m_database_wc_grants;

    /** Maps "user@host" to allowed databases. Retrieved from mysql.tables_priv, mysql.columns_priv and
     * mysql.procs_priv. No wildcards. */
    StringSetMap m_database_grants;

    /** Maps "user@host" to allowed roles. Retrieved from mysql.roles_mapping. */
    StringSetMap m_roles_mapping;

    StringSet m_database_names;     /**< Set with existing database names */
};

class MariaDBUserManager : public mxs::UserAccountManager
{
public:
    using SUserDB = std::shared_ptr<const UserDatabase>;
    MariaDBUserManager();
    ~MariaDBUserManager() override = default;

    /**
     * Start the updater thread. Should only be called when the updater is stopped or has just been created.
     */
    void start() override;

    /**
     * Stop the updater thread. Should only be called when the updater is running.
     */
    void stop() override;

    void update_user_accounts() override;
    void set_credentials(const std::string& user, const std::string& pw) override;
    void set_backends(const std::vector<SERVER*>& backends) override;
    void set_union_over_backends(bool union_over_backends) override;
    void set_strip_db_esc(bool strip_db_esc) override;
    void set_user_accounts_file(const std::string& filepath, UsersFileUsage file_usage) override;
    void set_service(SERVICE* service) override;

    bool can_update_immediately() const;

    std::unique_ptr<mxs::UserAccountCache> create_user_account_cache() override;

    std::string protocol_name() const override;
    json_t*     users_to_json() const override;

    struct UserDBInfo
    {
        SUserDB user_db;
        int     version {0};
    };

    /**
     * Get database info.
     *
     * @return Database pointer and version
     */
    UserDBInfo get_user_database() const;

    int      userdb_version() const;
    SERVICE* service() const;

private:
    using QResult = std::unique_ptr<mxq::QueryResult>;
    enum class LoadResult
    {
        SUCCESS,
        QUERY_FAILED,
        INVALID_DATA,
    };

    bool update_users();

    struct UserLoadRes
    {
        bool        success {false};
        std::string msg;
    };
    UserLoadRes load_users_from_backends(std::string&& conn_user, std::string&& conn_pw,
                                         std::vector<SERVER*>&& backends, UserDatabase& temp_userdata);
    UserLoadRes load_users_from_file(const std::string& source, UserDatabase* output);

    LoadResult load_users_mariadb(mxq::MariaDB& conn, SERVER* srv, UserDatabase* output);
    LoadResult load_users_xpand(mxq::MariaDB& con, SERVER* srv, UserDatabase* output);

    void updater_thread_function();

    bool read_users_mariadb(QResult users, const SERVER::VersionInfo& srv_info,
                            UserDatabase* output);
    void read_dbs_and_roles_mariadb(QResult db_wc_grants, QResult db_grants, QResult roles,
                                    UserDatabase* output);
    void read_proxy_grants(QResult proxies, UserDatabase* output);
    void read_databases(QResult dbs, UserDatabase* output);

    bool read_users_xpand(QResult users, UserDatabase* output);
    void read_db_privs_xpand(QResult acl, UserDatabase* output);

    void check_show_dbs_priv(mxq::MariaDB& con, const UserDatabase& userdata,
                             SERVER::VersionInfo::Type type, const char* servername);

    static void remove_star(std::string& pw);

    mutable std::mutex m_userdb_lock;       /**< Protects UserDatabase from concurrent access */
    SUserDB            m_userdb;            /**< Contains user account info */
    std::atomic_int    m_userdb_version {0};/**< How many times the user database has changed */

    // Fields for controlling the updater thread.
    std::thread             m_updater_thread;
    std::atomic_bool        m_keep_running {false};
    std::condition_variable m_notifier;
    std::mutex              m_notifier_lock;
    std::atomic_bool        m_update_users_requested {false};

    mxb::Semaphore m_thread_started;    /* Communicates that the updater thread has properly started. */

    // Settings and options. Access to arraylike fields is protected by the mutex.
    std::mutex           m_settings_lock;
    std::string          m_username;
    std::string          m_password;
    std::vector<SERVER*> m_backends;
    SERVICE*             m_service {nullptr};   /**< Service using this account data manager */

    /** User accounts file related settings. */
    std::string    m_users_file_path;
    UsersFileUsage m_users_file_usage {UsersFileUsage::ADD_WHEN_LOAD_OK};

    /** Fetch users from all backends and store the union. */
    std::atomic_bool m_union_over_backends {false};
    /** Remove escape characters '\' from database names when fetching user info from backend. */
    std::atomic_bool m_strip_db_esc {true};

    std::atomic_bool m_can_update {false};      /**< User accounts can or are about to be updated */
    int              m_successful_loads {0};    /**< Successful refreshes */

    /** How many times user loading has continuously failed. User for suppressing error messages. */
    int m_consecutive_failed_loads {0};

    /** Warn if no valid servers to query from. Starts false, as in the beginning monitors may not have
     *  ran yet. */
    std::atomic_bool m_warn_no_servers {false};

    /** Check if service user has "show databases" privilege. If found, not done again. */
    bool m_check_showdb_priv {true};
};

class MariaDBUserCache : public mxs::UserAccountCache
{
public:
    MariaDBUserCache(const MariaDBUserManager& master);
    ~MariaDBUserCache() override = default;

    /**
     * Check if user@host exists and can access the requested database. Does not check password or
     * any other authentication credentials.
     *
     * To roughly emulate server behavior, an entry is returned even if username was not found or
     * does not have access to database. This is so that a fake authentication exchange can be carried
     * out. Only if user gives the correct password the real error is returned.
     *
     * @param user Client username
     * @param ip Client ip
     * @param requested_db Database requested by client. May be empty.
     * @param sett User search settings
     * @return Result of the search
     */
    mariadb::UserEntryResult
    find_user(const std::string& user, const std::string& ip, const std::string& requested_db,
              const mariadb::UserSearchSettings& sett) const;

    void update_from_master() override;
    bool can_update_immediately() const;
    int  version() const;

private:
    void generate_dummy_entry(const std::string& user, mariadb::UserEntry* output) const;

    const MariaDBUserManager& m_master;     /**< User database master copy */

    MariaDBUserManager::SUserDB m_userdb;               /**< Local pointer to user database */
    int                         m_userdb_version {0};   /**< Version of local copy */
};
