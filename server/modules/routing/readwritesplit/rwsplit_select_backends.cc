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

#include "rwsplitsession.hh"

using namespace maxscale;

using std::chrono::duration_cast;
using std::chrono::microseconds;

namespace
{

/**
 * Check whether it's possible to use this server as a slave
 *
 * @param server The slave candidate
 * @param master The master server or nullptr if no master is available
 *
 * @return True if this server is a valid slave candidate
 */
bool valid_for_slave(const RWBackend* backend, const RWBackend* master)
{
    return (backend->is_slave() || backend->is_relay()) && (!master || backend != master);
}

/**
 * Check if replication lag is below acceptable levels
 */
bool rpl_lag_is_ok(mxs::RWBackend* backend, int max_rlag)
{
    bool ok = true;

    if (max_rlag != mxs::Target::RLAG_UNDEFINED)
    {
        auto rlag = backend->target()->replication_lag();
        ok = rlag != mxs::Target::RLAG_UNDEFINED && rlag < max_rlag;
    }

    return ok;
}

bool gtid_pos_is_ok(mxs::RWBackend* backend, RWSplit::gtid gtid_pos)
{
    return gtid_pos.sequence == 0 || backend->target()->gtid_pos(gtid_pos.domain) >= gtid_pos.sequence;
}

template<class Score>
RWBackend* best_score(const Candidates& sBackends, Score server_score)
{
    if (sBackends.empty())
    {
        return nullptr;
    }
    else if (sBackends.size() == 1)
    {
        return sBackends.front();
    }

    mxb_assert(sBackends.size() > 1);
    auto it = sBackends.begin();
    auto min = server_score(*it);
    RWBackend* best = *it;
    ++it;

    do
    {
        RWBackend* b = *it++;
        auto score = server_score(b);

        if (min > score)
        {
            min = score;
            best = b;
        }
        else if (min == score && b->last_write() < best->last_write())
        {
            // In the case of a tie, use the least recently used backend
            best = b;
        }
    }
    while (it != sBackends.end());

    return best;
}

/** Compare number of global connections in backend servers */
RWBackend* backend_cmp_global_conn(const Candidates& sBackends)
{
    auto server_score = [](RWBackend* b) {
        return b->target()->stats().n_current_conns();
    };

    return best_score(sBackends, server_score);
}

/** Compare replication lag between backend servers */
RWBackend* backend_cmp_behind_master(const Candidates& sBackends)
{
    static auto server_score = [](RWBackend* b) {
        return b->target()->replication_lag();
    };

    return best_score(sBackends, server_score);
}

/** Compare number of current operations in backend servers */
RWBackend* backend_cmp_current_load(const Candidates& sBackends)
{
    auto server_score = [](RWBackend* b) {
        return b->target()->stats().n_current_ops();
    };

    return best_score(sBackends, server_score);
}

/**
 * @brief Select a server based on current response averages and server load (#of active queries)
 *
 * The algorithm does this:
 * 1. Read the query time average of the servers into an array,
 * 2. Calculate the time it would take each server to processes the queued up
 *    queries plus one. estimate[i] = ave[i] * (n_current_ops + 1)
 * 3. Pick the server that would finish first if averages remained the same
 *
 * @param sBackends
 * @return the selected backend.
 */
RWBackend* backend_cmp_response_time(const Candidates& pBackends)
{
    if (pBackends.empty())
    {
        return nullptr;
    }
    else if (pBackends.size() == 1)
    {
        return pBackends.front();
    }

    mxb_assert (pBackends.size() > 1);
    const size_t SZ = pBackends.size();
    double estimated_time[SZ];

    // Calculate estimated time to finish current active messages + 1, per server
    for (size_t i {}; i < SZ; ++i)
    {
        estimated_time[i] = pBackends[i]->target()->response_time_average();
        estimated_time[i] += estimated_time[i] * pBackends[i]->target()->stats().n_current_ops();
        pBackends[i]->sync_averages();
    }

    // Pick the server that would finish first
    auto it = std::min_element(estimated_time, estimated_time + SZ);
    size_t index = it - estimated_time;

    mxb_assert(index < pBackends.size());
    return pBackends[index];
}

// Calculates server priority
int get_backend_priority(RWBackend* backend, uint64_t status, bool masters_accepts_reads)
{
    int priority;
    bool is_busy = backend->should_ignore_response();
    bool acts_slave = status & (SERVER_SLAVE | (masters_accepts_reads ? SERVER_MASTER : 0));

    if (acts_slave)
    {
        if (!is_busy)
        {
            priority = 0;   // Highest priority, idle servers
        }
        else
        {
            priority = 1;   // Lower priority, busy servers
        }
    }
    else
    {
        priority = 2;   // Lowest priority, idle masters with masters_accept_reads==false
    }

    return priority;
}
}

// static
BackendSelectFunction RWSConfig::get_backend_select_function(select_criteria_t sc)
{
    switch (sc)
    {
    case LEAST_GLOBAL_CONNECTIONS:
    case LEAST_ROUTER_CONNECTIONS:
        return backend_cmp_global_conn;

    case LEAST_BEHIND_MASTER:
        return backend_cmp_behind_master;

    case LEAST_CURRENT_OPERATIONS:
        return backend_cmp_current_load;

    case ADAPTIVE_ROUTING:
        return backend_cmp_response_time;
    }

    mxb_assert_message(false, "incorrect use of select_criteria_t");
    return backend_cmp_current_load;
}

int get_slave_counts(PRWBackends& backends, RWBackend* master)
{
    int slaves_connected = 0;

    /** Calculate how many connections we already have */
    for (const auto& backend : backends)
    {
        if (backend->in_use() && valid_for_slave(backend, master))
        {
            slaves_connected += 1;
        }
    }

    return slaves_connected;
}

bool RWSplitSession::is_gtid_synced(mxs::RWBackend* backend)
{
    bool rv = true;

    if (m_config->causal_reads == CausalReads::FAST || m_config->causal_reads == CausalReads::FAST_UNIVERSAL)
    {
        rv = gtid_pos_is_ok(backend, m_gtid_pos);
    }
    else if (m_config->causal_reads == CausalReads::FAST_GLOBAL)
    {
        rv = true;

        for (auto [domain, gtid] : m_router->last_gtid_map())
        {
            if (!gtid_pos_is_ok(backend, gtid))
            {
                rv = false;
            }
        }
    }

    return rv;
}

int64_t RWSplitSession::get_current_rank()
{
    int64_t rv = 1;

    if (m_current_master && m_current_master->in_use())
    {
        rv = m_current_master->target()->rank();
    }
    else
    {
        auto compare = [](RWBackend* a, RWBackend* b) {
            if (a->in_use() != b->in_use())
            {
                return a->in_use();
            }
            else if (a->can_connect() != b->can_connect())
            {
                return a->can_connect();
            }
            else
            {
                return a->target()->rank() < b->target()->rank();
            }
        };
        auto it = std::min_element(m_raw_backends.begin(), m_raw_backends.end(), compare);

        if (it != m_raw_backends.end())
        {
            rv = (*it)->target()->rank();
        }
    }

    return rv;
}

bool RWSplitSession::need_slaves()
{
    return get_slave_counts(m_raw_backends, m_current_master) < m_config->max_slave_connections;
}

RWBackend* RWSplitSession::get_slave_backend(int max_rlag)
{
    Candidates candidates;
    int best_priority {INT_MAX};
    auto current_rank = get_current_rank();

    // Create a list of backends valid for read operations
    for (auto& backend : m_raw_backends)
    {
        // We can take the current master back into use even for reads
        bool my_master = backend == m_current_master;
        bool already_used = backend->in_use();
        bool can_take_into_use = !already_used && can_recover_servers() && backend->can_connect();
        auto status = backend->target()->status();
        bool master_or_slave = status & (SERVER_MASTER | SERVER_SLAVE);
        bool in_maint = status & SERVER_MAINT;

        // The server is usable if it's already in use or it can be taken into use and we need either more
        // slaves or a master. Slaves can be taken into use if we need more slave connections.
        bool is_usable = already_used || (can_take_into_use && (need_slaves() || my_master));
        bool rlag_ok = rpl_lag_is_ok(backend, max_rlag);
        int priority = get_backend_priority(backend, status, m_config->master_accept_reads);
        auto rank = backend->target()->rank();
        bool gtid_is_ok = my_master || is_gtid_synced(backend);
        bool same_rank = rank == current_rank;
        m_check_stale = already_used && (in_maint || !same_rank);

        if (master_or_slave && !in_maint && is_usable && rlag_ok && same_rank && gtid_is_ok)
        {
            if (priority < best_priority)
            {
                candidates.clear();
                best_priority = priority;
            }

            if (priority == best_priority)
            {
                candidates.push_back(backend);
            }
        }

        if (max_rlag != mxs::Target::RLAG_UNDEFINED)
        {
            auto state = rlag_ok ? mxs::RLagState::BELOW_LIMIT : mxs::RLagState::ABOVE_LIMIT;
            backend->target()->set_rlag_state(state, max_rlag);
        }
    }

    // Let the slave selection function pick the best server
    return m_config->backend_select_fct(candidates);
}

/**
 * @brief Log server connections
 *
 * @param criteria Slave selection criteria
 * @param rses     Router client session
 */
static void log_server_connections(select_criteria_t criteria, const PRWBackends& backends)
{
    using maxbase::operator<<;

    MXB_INFO("Target connection counts:");

    for (auto b : backends)
    {
        switch (criteria)
        {
        case LEAST_GLOBAL_CONNECTIONS:
        case LEAST_ROUTER_CONNECTIONS:
            MXB_INFO("MaxScale connections : %ld in \t%s %s",
                     b->target()->stats().n_current_conns(),
                     b->name(), b->target()->status_string().c_str());
            break;

        case LEAST_CURRENT_OPERATIONS:
            MXB_INFO("current operations : %ld in \t%s %s",
                     b->target()->stats().n_current_ops(),
                     b->name(), b->target()->status_string().c_str());
            break;

        case LEAST_BEHIND_MASTER:
            MXB_INFO("replication lag : %ld in \t%s %s",
                     b->target()->replication_lag(),
                     b->name(), b->target()->status_string().c_str());
            break;

        case ADAPTIVE_ROUTING:
            {
                maxbase::Duration response_ave(mxb::from_secs(b->target()->response_time_average()));
                std::ostringstream os;
                os << response_ave;
                MXB_INFO("adaptive avg. select time: %s from \t%s %s",
                         os.str().c_str(), b->name(), b->target()->status_string().c_str());
            }
            break;

        default:
            mxb_assert(!true);
            break;
        }
    }
}

RWBackend* RWSplitSession::get_root_master()
{
    if (m_current_master && m_current_master->in_use() && m_current_master->is_master())
    {
        return m_current_master;
    }

    Candidates candidates;
    auto best_rank = std::numeric_limits<int64_t>::max();

    for (const auto& backend : m_raw_backends)
    {
        if (backend->can_connect() && backend->is_master())
        {
            auto rank = backend->target()->rank();

            if (rank < best_rank)
            {
                best_rank = rank;
                candidates.clear();
            }

            if (rank == best_rank)
            {
                candidates.push_back(backend);
            }
        }
    }

    return backend_cmp_global_conn(candidates);
}

/**
 * Select and connect to backend servers
 */
void RWSplitSession::open_connections()
{
    if (m_backends.empty())
    {
        throw RWSException("Service has no servers");
    }

    if (m_config->lazy_connect)
    {
        return;     // No need to create connections
    }

    RWBackend* master = get_root_master();

    if ((!master || !master->can_connect()) && m_config->master_failure_mode == RW_FAIL_INSTANTLY)
    {
        if (!master)
        {
            throw RWSException(mxb::string_printf(
                "Couldn't find suitable Primary from %lu candidates.", m_raw_backends.size()));
        }
        else
        {
            throw RWSException(mxb::string_printf(
                "Primary exists (%s), but it is being drained and cannot be used.", master->name()));
        }
    }

    if (mxb_log_should_log(LOG_INFO))
    {
        log_server_connections(m_config->slave_selection_criteria, m_raw_backends);
    }

    if (can_recover_servers())
    {
        // A master connection can be safely attempted
        if (master && !master->in_use() && master->can_connect())
        {
            prepare_connection(master);
            MXB_INFO("Selected Primary: %s", master->name());
            m_current_master = master;
        }
    }

    int n_slaves = get_slave_counts(m_raw_backends, master);
    int max_nslaves = std::min(m_config->max_slave_connections, m_config->slave_connections);
    mxb_assert(n_slaves <= max_nslaves || max_nslaves == 0);
    auto current_rank = get_current_rank();
    Candidates candidates;

    for (auto& backend : m_raw_backends)
    {
        if (!backend->in_use() && backend->can_connect() && valid_for_slave(backend, master)
            && backend->target()->rank() == current_rank
            && rpl_lag_is_ok(backend, get_max_replication_lag()))
        {
            candidates.push_back(backend);
        }
    }

    auto func = backend_cmp_global_conn;

    for (auto candidate = func(candidates);
         n_slaves < max_nslaves && !candidates.empty() && candidate;
         candidate = func(candidates))
    {
        try
        {
            prepare_connection(candidate);
            MXB_INFO("Selected Replica: %s", candidate->name());
            ++n_slaves;
        }
        catch (const RWSException& e)
        {
            MXB_INFO("Failed to connect to '%s': %s", candidate->name(), e.what());
        }

        candidates.erase(std::find(candidates.begin(), candidates.end(), candidate));
    }
}
