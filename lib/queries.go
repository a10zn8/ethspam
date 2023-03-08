package ethspam

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// TODO: Replace with proper JSON serialization? Originally was written to be quick&dirty for maximum perf.

func genEthCall(s State) string {
	// We eth_call the block before the call actually happened to avoid collision reverts
	to, from, input, block := s.RandomCall()
	if to != "" {
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_call","params":[{"to":%q,"from":%q,"data":%q},"0x%x"]}`+"\n", s.ID(), to, from, input, block-1)
	} else {
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_call","params":[{"from":%q,"data":%q},"0x%x"]}`+"\n", s.ID(), from, input, block-1)
	}
}

func genEthGetTransactionReceipt(s State) string {
	txID := s.RandomTransaction()
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_getTransactionReceipt","params":["%s"]}`+"\n", s.ID(), txID)
}

func genEthGetBalance(s State) string {
	addr := s.RandomAddress()
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_getBalance","params":["%s","latest"]}`+"\n", s.ID(), addr)
}

func genEthGetBalanceArchive(s State) string {
	addr := s.RandomAddress()
	r := s.RandInt64()
	blockNum := s.CurrentBlock() - uint64(r%100) - 200
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_getBalance","params":["%s","0x%x"]}`+"\n", s.ID(), addr, blockNum)
}

func genEthGetBlockByNumber(s State) string {
	r := s.RandInt64()
	blockNum := s.CurrentBlock() - uint64(r%5) // Within the last ~minute
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_getBlockByNumber","params":["0x%x",%s]}`+"\n", s.ID(), blockNum, false)
}

func genEthGetBlockByNumberFull(s State) string {
	r := s.RandInt64()
	blockNum := s.CurrentBlock() - uint64(r%5) // Within the last ~minute
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_getBlockByNumber","params":["0x%x",%s]}`+"\n", s.ID(), blockNum, true)
}

func genEthGetTransactionCount(s State) string {
	addr := s.RandomAddress()
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_getTransactionCount","params":["%s","pending"]}`+"\n", s.ID(), addr)
}

func genEthBlockNumber(s State) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_blockNumber"}`+"\n", s.ID())
}

func genEthGetTransactionByHash(s State) string {
	txID := s.RandomTransaction()
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_getTransactionByHash","params":["%s"]}`+"\n", s.ID(), txID)
}

func genEthGetLogs(s State) string {
	r := s.RandInt64()
	// TODO: Favour latest/recent block on a curve
	fromBlock := s.CurrentBlock() - uint64(r%5000) // Pick a block within the last ~day
	toBlock := s.CurrentBlock() - uint64(r%5)      // Within the last ~minute
	address, topics := s.RandomContract()
	topicsJoined := strings.Join(topics, `","`)
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_getLogs","params":[{"fromBlock":"0x%x","toBlock":"0x%x","address":"%s","topics":["%s"]}]}`+"\n", s.ID(), fromBlock, toBlock, address, topicsJoined)
}

func genEthGetCode(s State) string {
	addr, _ := s.RandomContract()
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_getCode","params":["%s","latest"]}`+"\n", s.ID(), addr)
}

func genEthEstimateGas(s State) string {
	to, from, input, block := s.RandomCall()
	if to != "" {
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_estimateGas","params":[{"to":%q,"from":%q,"data":%q},"0x%x"]}`+"\n", s.ID(), to, from, input, block-1)
	} else {
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_estimateGas","params":[{"from":%q,"data":%q},"0x%x"]}`+"\n", s.ID(), from, input, block-1)
	}
}

func getEthGetBlockByHash(s State) string {
	block := s.RandomBlock()
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_getBlockByHash","params":["%s",false]}`+"\n", s.ID(), block)
}

func getEthGetBlockByHashFull(s State) string {
	block := s.RandomBlock()
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_getBlockByHash","params":["%s",true]}`+"\n", s.ID(), block)
}

func getEthGetTransactionByBlockNumberAndIndex(s State) string {
	r := s.RandInt64()
	blockNum := s.CurrentBlock() - uint64(r%100) - 200
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_getTransactionByBlockNumberAndIndex","params":["0x%x","0x%x"]}`+"\n", s.ID(), blockNum, r%5)
}

func getNetVersion(s State) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"net_version"}`+"\n", s.ID())
}

func getEthGasPrice(s State) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_gasPrice"}`+"\n", s.ID())
}

func getNetListening(s State) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"net_listening"}`+"\n", s.ID())
}

func getNetPeerCount(s State) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"net_peerCount"}`+"\n", s.ID())
}

func getEthSyncing(s State) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_syncing"}`+"\n", s.ID())
}

func getEthGetStorageAt(s State) string {
	addr := s.RandomAddress()
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_getStorageAt","params":["%s","0x0","latest"]}`+"\n", s.ID(), addr)
}

// deprecated in erigon
func getEthAccounts(s State) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_accounts"}`+"\n", s.ID())
}

func getEthChainId(s State) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_chainId"}`+"\n", s.ID())
}

func getEthProtocolVersion(s State) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_protocolVersion"}`+"\n", s.ID())
}

func getEthFeeHistory(s State) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_feeHistory","params":[%d, "latest", []]}`+"\n", s.ID(), s.RandInt64()%10)
}

func getEthMaxPriorityFeePerGas(s State) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_maxPriorityFeePerGas"}`+"\n", s.ID())
}

func getEthGetTransactionByBlockHashAndIndex(s State) string {
	r := s.RandInt64()
	hash := s.RandomBlock()
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_getTransactionByBlockHashAndIndex","params":["0x%x","0x%x"]}`+"\n", s.ID(), hash, r%5)
}

func getEthGetBlockTransactionCountByHash(s State) string {
	hash := s.RandomBlock()
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_getBlockTransactionCountByHash","params":["0x%x"]}`+"\n", s.ID(), hash)
}

func getEthGetBlockTransactionCountByNumber(s State) string {
	block := s.CurrentBlock() - uint64(s.RandInt64()%100)
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_getBlockTransactionCountByNumber","params":["0x%x"]}`+"\n", s.ID(), block)
}

func getEthGetBlockReceipts(s State) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_getBlockReceipts","params":["latest"]}`+"\n", s.ID())
}

func getTraceBlock(s State) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"trace_block","params":["latest"]}`+"\n", s.ID())
}

func getTraceTransaction(s State) string {
	hash := s.RandomTransaction()
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"trace_transaction","params":["%s"]}`+"\n", s.ID(), hash)
}

func getTraceReplayTransaction(s State) string {
	hash := s.RandomTransaction()
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"trace_replayTransaction","params":["%s", ["trace"]]}`+"\n", s.ID(), hash)
}

func getTraceReplayBlockTransactions(s State) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"trace_replayBlockTransactions","params":["latest", ["trace"]]}`+"\n", s.ID())
}

func getDebugTraceTransaction(s State) string {
	hash := s.RandomTransaction()
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"debug_traceTransaction","params":["%s", {"tracer": "callTracer"}]}`+"\n", s.ID(), hash)
}

func getDebugTraceBlockByNumber(s State) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"debug_traceBlockByNumber","params":["latest", {"tracer": "callTracer"}]}`+"\n", s.ID())
}

func getDebugTraceBlockByHash(s State) string {
	hash := s.RandomBlock()
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"debug_traceBlockByHash","params":["%s", {"tracer": "callTracer"}]}`+"\n", s.ID(), hash)
}

func MakeQueriesGenerator(methods map[string]int64) (gen QueriesGenerator, err error) {
	// Top queries by weight, pulled from a 5000 Infura query sample on Dec 2019.
	//     3 "eth_accounts"
	//     4 "eth_getStorageAt"
	//     4 "eth_syncing"
	//     7 "net_peerCount"
	//    12 "net_listening"
	//    14 "eth_gasPrice"
	//    16 "eth_sendRawTransaction"
	//    25 "net_version"
	//    30 "eth_getTransactionByBlockNumberAndIndex"
	//    38 "eth_getBlockByHash"
	//    45 "eth_estimateGas"
	//    88 "eth_getCode"
	//   252 "eth_getLogs"
	//   255 "eth_getTransactionByHash"
	//   333 "eth_blockNumber"
	//   390 "eth_getTransactionCount"
	//   399 "eth_getBlockByNumber"
	//   545 "eth_getBalance"
	//   607 "eth_getTransactionReceipt"
	//  1928 "eth_call"

	rpcMethod := map[string]func(State) string{
		"eth_call":                                genEthCall,
		"eth_getTransactionReceipt":               genEthGetTransactionReceipt,
		"eth_getBalance":                          genEthGetBalance,
		"eth_getBlockByNumber":                    genEthGetBlockByNumber,
		"eth_getBlockByNumber#full":               genEthGetBlockByNumberFull,
		"eth_getTransactionCount":                 genEthGetTransactionCount,
		"eth_blockNumber":                         genEthBlockNumber,
		"eth_getTransactionByHash":                genEthGetTransactionByHash,
		"eth_getLogs":                             genEthGetLogs,
		"eth_getCode":                             genEthGetCode,
		"eth_estimateGas":                         genEthEstimateGas,
		"eth_getBlockByHash":                      getEthGetBlockByHash,
		"eth_getBlockByHash#full":                 getEthGetBlockByHashFull,
		"eth_getTransactionByBlockNumberAndIndex": getEthGetTransactionByBlockNumberAndIndex,
		"net_version":                             getNetVersion,
		"eth_gasPrice":                            getEthGasPrice,
		"net_listening":                           getNetListening,
		"net_peerCount":                           getNetPeerCount,
		"eth_syncing":                             getEthSyncing,
		"eth_getStorageAt":                        getEthGetStorageAt,
		"eth_accounts":                            getEthAccounts,
		"eth_chainId":                             getEthChainId,
		"eth_protocolVersion":                     getEthProtocolVersion,
		"eth_feeHistory":                          getEthFeeHistory,
		"eth_maxPriorityFeePerGas":                getEthMaxPriorityFeePerGas,
		"eth_getTransactionByBlockHashAndIndex":   getEthGetTransactionByBlockHashAndIndex,
		"eth_getBlockTransactionCountByHash":      getEthGetBlockTransactionCountByHash,
		"eth_getBlockTransactionCountByNumber":    getEthGetBlockTransactionCountByNumber,
		"eth_getBlockReceipts":                    getEthGetBlockReceipts,
		"getTraceBlock":                           getTraceBlock,
		"getTraceTransaction":                     getTraceTransaction,
		"getTraceReplayTransaction":               getTraceReplayTransaction,
		"getTraceReplayBlockTransactions":         getTraceReplayBlockTransactions,
		"getDebugTraceTransaction":                getDebugTraceTransaction,
		"getDebugTraceBlockByNumber":              getDebugTraceBlockByNumber,
		"getDebugTraceBlockByHash":                getDebugTraceBlockByHash,
	}

	for method, weight := range methods {
		if weight == 0 {
			continue
		}
		if _, err := rpcMethod[method]; err == false {
			return QueriesGenerator{}, errors.New(method + " is not supported")
		}
		gen.Add(RandomQuery{
			Method:   method,
			Weight:   weight,
			Generate: rpcMethod[method],
		})
	}

	return gen, nil
}

type Generator func(State) string

type RandomQuery struct {
	Method   string
	Weight   int64
	Generate Generator
}

type QueriesGenerator struct {
	queries     []RandomQuery // sorted by weight asc
	totalWeight int64
}

// Add inserts a random query QueriesGenerator with a weighted probability. Not
// goroutine-safe, should be run once during initialization.
func (g *QueriesGenerator) Add(query RandomQuery) {
	if g.queries == nil {
		g.queries = make([]RandomQuery, 1)
	} else {
		g.queries = append(g.queries, RandomQuery{})
	}
	// Maintain weight sort
	idx := sort.Search(len(g.queries), func(i int) bool { return g.queries[i].Weight < query.Weight })
	copy(g.queries[idx+1:], g.queries[idx:])
	g.queries[idx] = query
	g.totalWeight += query.Weight
}

// Query selects a QueriesGenerator based on proportonal weighted probability and
// writes the query from the QueriesGenerator.
func (g *QueriesGenerator) Query(s State) (string, error) {
	if len(g.queries) == 0 {
		return "", errors.New("no query generators available")
	}

	weight := s.RandInt64() % g.totalWeight

	var current int64
	for _, q := range g.queries {
		current += q.Weight
		if current >= weight {
			return q.Generate(s), nil
		}
	}

	panic("off by one bug in weighted query selection")
}
