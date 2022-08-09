/*
 * Copyright (c) 2020 MariaDB Corporation Ab
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2026-08-08
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
import Vue from 'vue'
import Vuex from 'vuex'
import user from 'store/user'
import maxscale from './maxscale'
import server from './server'
import service from './service'
import monitor from './monitor'
import filter from './filter'
import session from './session'
import listener from './listener'
import queryEditorModules from './queryEditor'
import persisted from './persisted'
import visualization from './visualization'
import { APP_CONFIG } from 'utils/constants'
import router from 'router'
import i18n from 'plugins/i18n'
import VuexPersistence from 'vuex-persist'
import localForage from 'localforage'

import { refreshAxiosToken, cancelAllRequests, authHttp, http, queryHttp } from 'utils/axios'
const plugins = store => {
    store.router = router
    store.vue = Vue.prototype
    store.i18n = i18n
    store.$authHttp = authHttp
    store.$http = http(store)
    store.$queryHttp = queryHttp(store)
    store.$refreshAxiosToken = refreshAxiosToken
    store.$cancelAllRequests = cancelAllRequests
}

const vuexLocalForage = new VuexPersistence({
    key: 'maxgui',
    storage: localForage,
    asyncStorage: true,
    reducer: state => ({
        persisted: state.persisted,
        wke: { worksheets_arr: state.wke.worksheets_arr, active_wke_id: state.wke.active_wke_id },
        queryConn: { sql_conns: state.queryConn.sql_conns },
        querySession: {
            query_sessions: state.querySession.query_sessions,
            active_session_by_wke_id_map: state.querySession.active_session_by_wke_id_map,
        },
        user: { logged_in_user: state.user.logged_in_user },
    }),
})

const store = new Vuex.Store({
    plugins: [plugins, vuexLocalForage.plugin],
    state: {
        app_config: APP_CONFIG,
        snackbar_message: {
            status: false,
            text: '',
            type: 'info',
        },
        search_keyword: '',
        overlay_type: false,
        update_availability: false,
        prev_route: null,
        module_parameters: [],
        form_type: '',
        should_refresh_resource: false,
    },
    mutations: {
        SET_OVERLAY_TYPE(state, type) {
            state.overlay_type = type
        },
        /**
         * @param {Object} obj Object snackbar_message
         * @param {Array} obj.text An array of string
         * @param {String} obj.type Type of response
         */
        SET_SNACK_BAR_MESSAGE(state, obj) {
            const { text, type, status = true } = obj
            state.snackbar_message.status = status
            state.snackbar_message.text = text
            state.snackbar_message.type = type
        },
        /**
         * @param {String} keyword global search keyword
         */
        SET_SEARCH_KEYWORD(state, keyword) {
            state.search_keyword = keyword
        },
        SET_UPDATE_AVAILABILITY(state, val) {
            state.update_availability = val
        },
        SET_PREV_ROUTE(state, prev_route) {
            state.prev_route = prev_route
        },
        SET_MODULE_PARAMETERS(state, module_parameters) {
            state.module_parameters = module_parameters
        },
        SET_FORM_TYPE(state, form_type) {
            state.form_type = form_type
        },
        SET_REFRESH_RESOURCE(state, boolean) {
            state.should_refresh_resource = boolean
        },
    },
    actions: {
        /**
         * User will be logged out if maxscale is restarted or maxgui is updated
         * This action checks if an update is available.
         * It should be dispatched on public route when routing occurs
         */
        async checkingForUpdate({ commit }) {
            this.$refreshAxiosToken()
            const logger = this.vue.$logger('index-store')
            const res = await this.$http.get(`/`)
            logger.info('Checking for update')
            const resDoc = new DOMParser().parseFromString(res.data, 'text/html')
            const newCommitId = resDoc.getElementsByName('commitId')[0].content
            const currentAppCommitId = document
                .getElementsByName('commitId')[0]
                .getAttribute('content')
            logger.info('MaxGUI commit id:', currentAppCommitId)
            logger.info('MaxGUI new commit id:', newCommitId)
            if (currentAppCommitId !== newCommitId) {
                commit('SET_UPDATE_AVAILABILITY', true)
                logger.info('New version is available')
            }
        },
        /**
         * This function fetch all resources state, if resourceId is not provided,
         * otherwise it fetch resource state of a resource based on resourceId
         * @param {String} resourceId id of the resource
         * @param {String} resourceType type of resource. e.g. servers, services, monitors
         * @param {String} caller name of the function calling this function, for debugging purpose
         * @return {Array} Resource state data
         */
        async getResourceState(_, { resourceId, resourceType, caller }) {
            try {
                let data = []
                let res
                if (resourceId) {
                    res = await this.$http.get(
                        `/${resourceType}/${resourceId}?fields[${resourceType}]=state`
                    )
                } else res = await this.$http.get(`/${resourceType}?fields[${resourceType}]=state`)

                if (res.data.data) data = res.data.data
                return data
            } catch (e) {
                const logger = this.vue.$logger(caller)
                logger.error(e)
            }
        },

        async fetchModuleParameters({ commit }, moduleId) {
            try {
                let data = []
                let res = await this.$http.get(
                    `/maxscale/modules/${moduleId}?fields[modules]=parameters`
                )
                if (res.data.data) {
                    const { attributes: { parameters = [] } = {} } = res.data.data
                    data = parameters
                }
                commit('SET_MODULE_PARAMETERS', data)
            } catch (e) {
                const logger = this.vue.$logger(`fetchModuleParameters-for-${moduleId}`)
                logger.error(e)
            }
        },
    },
    modules: {
        filter,
        listener,
        maxscale,
        monitor,
        server,
        service,
        session,
        user,
        ...queryEditorModules,
        persisted,
        visualization,
    },
})
export default store
