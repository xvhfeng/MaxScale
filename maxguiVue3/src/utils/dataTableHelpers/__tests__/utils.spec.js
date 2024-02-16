/*
 * Copyright (c) 2023 MariaDB plc
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
import * as utils from '@/utils/dataTableHelpers/utils'
import * as mockData from '@/utils/mockData'

describe('dataTableHelpers utils', () => {
  describe('isNotEmptyObj assertions', () => {
    for (const [key, value] of Object.entries(mockData.mixedTypeValues)) {
      let expectResult = key === 'validObj'
      it(`Should return ${expectResult} when value is ${key}`, () => {
        expect(utils.isNotEmptyObj(value)).to.be[expectResult]
      })
    }
  })

  describe('isNotEmptyArray assertions', () => {
    for (const [key, value] of Object.entries(mockData.mixedTypeValues)) {
      let expectResult = key === 'validArr' || key === 'validArrObj'
      it(`Should return ${expectResult} when value is ${key}`, () => {
        expect(utils.isNotEmptyArray(value)).to.be[expectResult]
      })
    }
  })

  it(`Should return flattened tree when flattenExpandableTree is called`, () => {
    const flattened = utils.flattenExpandableTree(mockData.treeNodes)
    const lastNodeId = mockData.treeNodes[mockData.treeNodes.length - 1].id
    expect(flattened.length).to.be.equals(lastNodeId)
    flattened.forEach((node) => {
      if (node.children) expect(node.expanded).to.be.true
    })
  })

  it(`Should return ancestor id of a node when findAncestor is called`, () => {
    const expectAncestorNodeId = mockData.treeNodes[0].id
    const nodeStub = mockData.treeNodes[0].children[0].children[0]
    let treeMapMock = new Map()
    const flattened = utils.flattenExpandableTree(mockData.treeNodes)
    flattened.forEach((node) => treeMapMock.set(node.id, node))
    const ancestorId = utils.findAncestor({ node: nodeStub, treeMap: treeMapMock })
    expect(ancestorId).to.be.equals(expectAncestorNodeId)
  })

  it(`Should update node at depth level when updateNode is called`, () => {
    let objToBeUpdated = {
      root_node: {
        node_child: {
          grand_child: 'grand_child value',
          grand_child_1: 'grand_child_1 value',
        },
        node_child_1: 'node_child_1 value',
      },
    }
    const expectResult = {
      root_node: {
        node_child: {
          grand_child: 'grand_child value',
          grand_child_1: 'new grand_child_1 value',
        },
        node_child_1: 'node_child_1 value',
      },
    }
    utils.updateNode({
      obj: objToBeUpdated,
      node: {
        id: 'grand_child_1',
        value: 'new grand_child_1 value',
      },
    })
    expect(objToBeUpdated).to.be.deep.equals(expectResult)
  })
})
