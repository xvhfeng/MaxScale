/*
 * Copyright (c) 2024 MariaDB plc
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

/**
 * Test arbitrary configuration generation with multi-layered services
 */

#include <maxtest/testconnections.hh>
#include <maxbase/json.hh>
#include <random>

static std::atomic<bool> running {true};
static std::minstd_rand0 seq(123456U);

class StsTester
{
public:
    using ConfigGenerator = std::function<std::string ()>;

    StsTester(TestConnections& test)
        : m_test(test)
    {
        // Create the root service and a listener for it
        auto service = next_service();
        cmd("create service " + service + " readwritesplit "
            + credentials() + " targets=server1,server2,server3,server4");
        cmd("create listener " + service + " listener0 4006");

        m_rels[service] = {"server1", "server2", "server3", "server4"};
    }

    void add_service()
    {
        auto victim = m_services[seq() % m_services.size()];
        create_service(victim, m_rels[victim]);
    }

    void remove_service()
    {
        auto victim = m_services[seq() % m_services.size()];

        // Don't destroy the root service
        if (victim != m_services.front())
        {
            destroy_service(find_parent(victim), victim, m_rels[victim]);
        }
    }

    void link_service()
    {
        auto svc = m_services[seq() % m_services.size()];
        auto target = m_services[seq() % m_services.size()];

        if (svc != target)
        {
            // Linking might fail if it would create a circular configuration
            if (try_cmd("link service " + svc + " " + target))
            {
                m_rels[svc].insert(target);
                m_test.tprintf("Link service '%s' to '%s'", svc.c_str(), target.c_str());
            }
        }
    }

private:
    void cmd(const std::string& arg)
    {
        m_test.check_maxctrl(arg);
    }

    bool try_cmd(const std::string& arg)
    {
        return m_test.maxctrl(arg).rc == 0;
    }

    std::string next_service()
    {
        std::string svc = "service" + std::to_string(m_next_service_id++);
        m_services.push_back(svc);
        return svc;
    }

    std::string next_filter()
    {
        std::string filter = "filter" + std::to_string(m_next_filter_id++);
        m_filters.push_back(filter);
        return filter;
    }
    std::string credentials() const
    {
        return "user=" + m_test.maxscale->user_name() + " password=" + m_test.maxscale->password();
    }

    std::string find_parent(const std::string& target)
    {
        for (const auto& parent_child : m_rels)
        {
            if (parent_child.second.find(target) != parent_child.second.end())
            {
                return parent_child.first;
            }
        }

        m_test.add_failure("Could not find parent for target '%s'", target.c_str());
        return "";
    }

    void create_service(const std::string& parent, std::set<std::string> children)
    {
        auto new_service = next_service();
        cmd("create service " + new_service + " " + random_router() + " " + credentials());

        // Move a random number of servers from the parent service to the newly created service.
        int replace_count = std::max(1UL, seq() % children.size());
        auto it = std::next(children.begin(), replace_count % children.size());
        std::set<std::string> new_children(it, children.end());
        children.erase(it, children.end());

        // Add the newly created service to the parent service.
        cmd("link service " + parent + " " + new_service);
        children.insert(new_service);

        cmd("alter service " + new_service + " targets=" + mxb::join(new_children));
        cmd("alter service " + parent + " targets=" + mxb::join(children));

        m_rels[new_service] = new_children;
        m_rels[parent] = children;

        m_test.tprintf("Create service '%s'", new_service.c_str());
    }

    void destroy_service(const std::string& parent, const std::string& victim, std::set<std::string> children)
    {
        auto new_children = m_rels[parent];
        new_children.erase(victim);
        new_children.insert(children.begin(), children.end());

        cmd("unlink service " + parent + " " + victim);

        for (const auto& child : children)
        {
            cmd("link service " + parent + " " + child);
        }

        cmd("destroy service " + victim + " --force");
        m_rels[parent] = new_children;
        m_rels.erase(victim);
        m_services.erase(std::remove(m_services.begin(), m_services.end(), victim), m_services.end());

        std::string empty_service;

        for (auto& kv : m_rels)
        {
            kv.second.erase(victim);

            if (kv.second.empty())
            {
                empty_service = kv.first;
            }
        }

        m_test.tprintf("Destroy service '%s'", victim.c_str());

        if (!empty_service.empty())
        {
            m_test.tprintf("Recurse to '%s'", empty_service.c_str());
            auto grandfather = find_parent(empty_service);
            destroy_service(grandfather, empty_service, m_rels[grandfather]);
        }
    }

    ConfigGenerator constant(std::string str)
    {
        return [str](){
            return str;
        };
    }

    std::string random_router()
    {
        std::array routers {
            constant("readwritesplit"),
            constant("readwritesplit transaction_replay=true"),
            constant("readwritesplit causal_reads=local"),
            constant("readconnroute router_options=running"),
            constant("readconnroute router_options=slave"),
            constant("schemarouter ignore_tables_regex=.*"),
        };

        std::uniform_int_distribution<> dist(0, routers.size() - 1);
        return routers[dist(seq)]();
    }

    ConfigGenerator create_qlafilter()
    {
        return [this](){
            return "qlafilter log_type=unified filebase=/tmp/qlalog."
                   + std::to_string(m_next_qlafilter_file++) + ".txt";
        };
    }

    std::string random_filter()
    {
        std::array filters {
            constant("cache storage=storage_inmemory storage=storage_inmemory cached_data=shared"),
            create_qlafilter(),
        };

        std::uniform_int_distribution<> dist(0, filters.size() - 1);
        return filters[dist(seq)]();
    }

    TestConnections&                             m_test;
    int                                          m_next_service_id {0};
    int                                          m_next_filter_id {0};
    int                                          m_next_qlafilter_file {0};
    std::vector<std::string>                     m_services;
    std::vector<std::string>                     m_filters;
    std::map<std::string, std::set<std::string>> m_rels;
};

void do_queries(TestConnections& test)
{
    while (running && test.ok())
    {
        auto c = test.maxscale->rwsplit();

        if (c.connect())
        {
            for (int i = 0; i < 5 && running && test.ok(); i++)
            {
                c.query("SELECT 1 + SLEEP(RAND())");
            }
        }
        else
        {
            test.tprintf("Failed to connect: %s", c.error());
        }
    }
}

void test_main(TestConnections& test)
{
    StsTester tester(test);
    std::vector<std::thread> threads;
    std::uniform_int_distribution<> dist(0, 100);

    for (int i = 0; i < 24; i++)
    {
        threads.emplace_back(do_queries, std::ref(test));
    }

    for (int i = 0; i < 1000 && test.ok(); i++)
    {
        test.reset_timeout();

        int dice_roll = dist(seq);

        if (dice_roll < 40)
        {
            tester.add_service();
        }
        else if (dice_roll < 80)
        {
            tester.link_service();
        }
        else
        {
            tester.remove_service();
        }
    }

    running = false;
    test.tprintf("Joining thredads...");

    for (auto& thr : threads)
    {
        thr.join();
    }
}

int main(int argc, char** argv)
{
    return TestConnections().run_test(argc, argv, test_main);
}
