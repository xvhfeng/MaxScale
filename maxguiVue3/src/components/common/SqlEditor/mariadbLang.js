/*
 * Copyright (c) 2020 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2028-01-30
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
import { mariadb } from 'sql-formatter'

const {
    reservedKeywords: keywords,
    reservedFunctionNames: builtinFunctions,
} = mariadb.tokenizerOptions

export const languageConfiguration = {
    comments: {
        lineComment: '--',
        blockComment: ['/*', '*/'],
    },
    brackets: [
        ['{', '}'],
        ['[', ']'],
        ['(', ')'],
    ],
    autoClosingPairs: [
        { open: '{', close: '}' },
        { open: '[', close: ']' },
        { open: '(', close: ')' },
        { open: '"', close: '"' },
        { open: "'", close: "'" },
    ],
    surroundingPairs: [
        { open: '{', close: '}' },
        { open: '[', close: ']' },
        { open: '(', close: ')' },
        { open: '"', close: '"' },
        { open: "'", close: "'" },
    ],
}

export const languageTokens = {
    defaultToken: '',
    tokenPostfix: '.mariadb',
    ignoreCase: false,
    brackets: [
        { open: '[', close: ']', token: 'delimiter.square' },
        { open: '(', close: ')', token: 'delimiter.parenthesis' },
    ],
    keywords,
    builtinFunctions,
    tokenizer: {
        root: [
            { include: '@comments' },
            { include: '@whitespace' },
            { include: '@numbers' },
            { include: '@strings' },
            { include: '@complexIdentifiers' },
            [/[;,.]/, 'delimiter'],
            [/[()]/, '@brackets'],
            [
                /[\w@]+/,
                {
                    cases: {
                        '@keywords': 'keyword',
                        '@builtinFunctions': 'predefined',
                        '@default': 'identifier',
                    },
                },
            ],
            [/[<>=!%&+\-*/|~^]/, 'operator'],
        ],
        whitespace: [[/\s+/, 'white']],
        comments: [
            [/--\s+.*/, 'comment'],
            [/#+.*/, 'comment'],
            [/\/\*/, { token: 'comment.quote', next: '@comment' }],
        ],
        comment: [
            [/[^*/]+/, 'comment'],
            [/\*\//, { token: 'comment.quote', next: '@pop' }],
            [/./, 'comment'],
        ],
        numbers: [
            [/0[xX][0-9a-fA-F]*/, 'number'],
            [/[$][+-]*\d*(\.\d*)?/, 'number'],
            // eslint-disable-next-line no-useless-escape
            [/((\d+(\.\d*)?)|(\.\d+))([eE][\-+]?\d+)?/, 'number'],
        ],
        strings: [
            [/'/, { token: 'string', next: '@string' }],
            [/"/, { token: 'string.double', next: '@stringDouble' }],
        ],
        string: [
            [/[^']+/, 'string'],
            [/''/, 'string'],
            [/'/, { token: 'string', next: '@pop' }],
        ],
        stringDouble: [
            [/[^"]+/, 'string.double'],
            [/""/, 'string.double'],
            [/"/, { token: 'string.double', next: '@pop' }],
        ],
        complexIdentifiers: [[/`/, { token: 'identifier.quote', next: '@quotedIdentifier' }]],
        quotedIdentifier: [
            [/[^`]+/, 'identifier'],
            [/``/, 'identifier'],
            [/`/, { token: 'identifier.quote', next: '@pop' }],
        ],
    },
}
