package shardgrp

import "6.5840/kvsrv1/rpc"

func (c *Cache) _isCommandRecentlyExecuted(arg rpc.CommonKVCommandsInterface) bool {
	v, ok := c.LastClientsReqs[arg.GetClientId()]
	if !ok {
		return false
	}
	return v == arg.GetRequestId()
}

func (c *Cache) _isCommandTooOld(arg rpc.CommonKVCommandsInterface) bool {
	v, ok := c.LastClientsReqs[arg.GetClientId()]
	if !ok {
		return false
	}
	return arg.GetRequestId() < v
}

func (c *Cache) _getCachedResponse(arg rpc.CommonKVCommandsInterface) any {
	v, ok := c.LastClientsResp[arg.GetClientId()]
	if !ok {
		panic("tried to retrieve unaccessible key")
	}
	return v
}

func (c *Cache) _saveReqIdAndResponse(args rpc.CommonKVCommandsInterface, response any) {
	c._updateRecentReqId(args)
	c._updateResponse(args, response)
}

func (c *Cache) _updateRecentReqId(arg rpc.CommonKVCommandsInterface) {
	c.LastClientsReqs[arg.GetClientId()] = arg.GetRequestId()
}

func (c *Cache) _updateResponse(args rpc.CommonKVCommandsInterface, response any) {
	c.LastClientsResp[args.GetClientId()] = response
}
