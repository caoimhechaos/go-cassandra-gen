package cassandra

import (
	"expvar"
	"io"
	"sync"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
)

// Statistics.
// The number of times we reconnected and retried a command.
var num_retries *expvar.Int = expvar.NewInt("cassandra-num-retries")

// The number of successfully completed operations.
var cassandra_num_ops *expvar.Map = expvar.NewMap("cassandra-operations-total-count")

// The number of nanoseconds it took to complete those operations.
var cassandra_ops_latency *expvar.Map = expvar.NewMap("cassandra-operations-latency-overall-sum")

// The number of errors which ocurred in each method, in addition to the
// number of successful operations.
var cassandra_num_errors *expvar.Map = expvar.NewMap("cassandra-operation-errors")

// Cassandra connection wrapper which inspects responses to typical Thrift
// RPCs and retries them as required.
type RetryCassandraClient struct {
	wrapped          *CassandraClient
	keyspace         string
	dbhost           string
	auth             *AuthenticationRequest
	protocolFactory  *thrift.TBinaryProtocolFactory
	transportFactory thrift.TTransportFactory
	transport        thrift.TTransport
	socket           *thrift.TSocket
	mtx              sync.RWMutex
}

// Determine if a given error can be solved by submitting the request again.
func IsRetryable(err error) bool {
	// EOFs can occur if the Cassandra server is restarted. In that case,
	// we should just contact it again and retry.
	if err != nil && err.Error() == io.EOF.Error() {
		return true
	}

	return false
}

// Create a new retry Cassandra wrapper connected to the given host.
func NewRetryCassandraClient(host string) (c *RetryCassandraClient,
	err error) {
	return NewRetryCassandraClientTimeout(host, 0)
}

func NewRetryCassandraClientTimeout(host string, timeout time.Duration) (
	c *RetryCassandraClient, err error) {
	var begin time.Time = time.Now()
	var protocolFactory *thrift.TBinaryProtocolFactory
	var socket *thrift.TSocket
	var transportFactory thrift.TTransportFactory

	protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()
	transportFactory = thrift.NewTFramedTransportFactory(
		thrift.NewTTransportFactory())
	if timeout > 0 {
		socket, err = thrift.NewTSocketTimeout(host, timeout)
	} else {
		socket, err = thrift.NewTSocket(host)
	}
	if err != nil {
		return
	}

	c = &RetryCassandraClient{
		protocolFactory:  protocolFactory,
		transportFactory: transportFactory,
		socket:           socket,
		dbhost:           host,
	}
	err = c.Reconnect()
	cassandra_ops_latency.Add("Connect", time.Now().UnixNano()-begin.UnixNano())
	cassandra_num_ops.Add("Connect", 1)
	return
}

// Reestablish the connection to the destination; should only be called if
// a method failed.
func (self *RetryCassandraClient) Reconnect() error {
	var transport thrift.TTransport
	var err error
	self.mtx.Lock()
	defer self.mtx.Unlock()

	if self.socket.IsOpen() {
		num_retries.Add(1)
		self.socket.Close()
	}

	if err = self.socket.Open(); err != nil {
		cassandra_num_errors.Add("connect", 1)
		return err
	}

	transport = self.transportFactory.GetTransport(self.socket)
	self.wrapped = NewCassandraClientFactory(transport, self.protocolFactory)

	if self.auth != nil {
		// TODO(tonnerre): Handle errors here.
		self.wrapped.Login(self.auth)
	}

	if len(self.keyspace) > 0 {
		// TODO(tonnerre): Handle errors here.
		self.wrapped.SetKeyspace(self.keyspace)
	}

	return nil
}

// Requests to log into the Cassandra service.
//
// Parameters:
//  - AuthRequest: request for authentication to the Cassandra service.
func (self *RetryCassandraClient) Login(
	auth_request *AuthenticationRequest) error {
	var begin time.Time = time.Now()
	self.auth = auth_request
	self.mtx.RLock()
	defer self.mtx.RUnlock()
	cassandra_num_ops.Add("Login", 1)
	defer cassandra_ops_latency.Add("Login", time.Now().UnixNano()-begin.UnixNano())
	return self.wrapped.Login(auth_request)
}

// Sets the keyspace to use for queries.
//
// Parameters:
//  - Keyspace
func (self *RetryCassandraClient) SetKeyspace(keyspace string) error {
	var begin time.Time = time.Now()
	self.keyspace = keyspace
	self.mtx.RLock()
	defer self.mtx.RUnlock()
	cassandra_num_ops.Add("SetKeyspace", 1)
	defer cassandra_ops_latency.Add("SetKeyspace",
		time.Now().UnixNano()-begin.UnixNano())
	return self.wrapped.SetKeyspace(keyspace)
}

// Get the Column or SuperColumn at the given column_path. If no value is
// present, NotFoundException is thrown. (This is the only method that can
// throw an exception under non-failure conditions.)
//
// Parameters:
//  - Key
//  - ColumnPath
//  - ConsistencyLevel
func (self *RetryCassandraClient) Get(key []byte, column_path *ColumnPath,
	consistency_level ConsistencyLevel) (r *ColumnOrSuperColumn, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.Get(key, column_path, consistency_level)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.Get(key, column_path, consistency_level)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("Get", 1)
	cassandra_ops_latency.Add("Get", time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("Get", 1)
	}
	return
}

// Get the group of columns contained by column_parent (either a ColumnFamily
// name or a ColumnFamily/SuperColumn name pair) specified by the given
// SlicePredicate. If no matching values are found, an empty list is returned.
//
// Parameters:
//  - Key
//  - ColumnParent
//  - Predicate
//  - ConsistencyLevel
func (self *RetryCassandraClient) GetSlice(
	key []byte, column_parent *ColumnParent, predicate *SlicePredicate,
	consistency_level ConsistencyLevel) (r []*ColumnOrSuperColumn, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.GetSlice(key, column_parent, predicate,
		consistency_level)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.GetSlice(key, column_parent,
			predicate, consistency_level)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("GetSlice", 1)
	cassandra_ops_latency.Add("GetSlice", time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("GetSlice", 1)
	}
	return
}

// returns the number of columns matching <code>predicate</code> for a
// particular <code>key</code>, <code>ColumnFamily</code> and optionally
// <code>SuperColumn</code>.
//
// Parameters:
//  - Key
//  - ColumnParent
//  - Predicate
//  - ConsistencyLevel
func (self *RetryCassandraClient) GetCount(key []byte,
	column_parent *ColumnParent, predicate *SlicePredicate,
	consistency_level ConsistencyLevel) (r int32, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.GetCount(key, column_parent, predicate,
		consistency_level)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.GetCount(key, column_parent, predicate,
			consistency_level)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("GetCount", 1)
	cassandra_ops_latency.Add("GetCount", time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("GetCount", 1)
	}
	return
}

// Performs a get_slice for column_parent and predicate for the given keys
// in parallel.
//
// Parameters:
//  - Keys
//  - ColumnParent
//  - Predicate
//  - ConsistencyLevel
func (self *RetryCassandraClient) MultigetSlice(keys [][]byte,
	column_parent *ColumnParent, predicate *SlicePredicate,
	consistency_level ConsistencyLevel) (r map[string][]*ColumnOrSuperColumn,
	err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.MultigetSlice(keys, column_parent, predicate,
		consistency_level)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.MultigetSlice(keys, column_parent, predicate,
			consistency_level)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("MultigetSlice", 1)
	cassandra_ops_latency.Add("MultigetSlice", time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("MultigetSlice", 1)
	}
	return
}

// Perform a get_count in parallel on the given list<binary> keys. The return value maps keys to the count found.
//
// Parameters:
//  - Keys
//  - ColumnParent
//  - Predicate
//  - ConsistencyLevel
func (self *RetryCassandraClient) MultigetCount(
	keys [][]byte, column_parent *ColumnParent, predicate *SlicePredicate,
	consistency_level ConsistencyLevel) (r map[string]int32,
	ire *InvalidRequestException, ue *UnavailableException,
	te *TimedOutException, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.MultigetCount(keys, column_parent, predicate,
		consistency_level)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.MultigetCount(keys, column_parent, predicate,
			consistency_level)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("MultigetCount", 1)
	cassandra_ops_latency.Add("MultigetCount",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("MultigetCount", 1)
	}
	return
}

// returns a subset of columns for a contiguous range of keys.
//
// Parameters:
//  - ColumnParent
//  - Predicate
//  - RangeA1
//  - ConsistencyLevel
func (self *RetryCassandraClient) GetRangeSlices(
	column_parent *ColumnParent, predicate *SlicePredicate,
	range_a1 *KeyRange, consistency_level ConsistencyLevel) (
	r []*KeySlice, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.GetRangeSlices(column_parent, predicate, range_a1,
		consistency_level)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.GetRangeSlices(column_parent, predicate, range_a1,
			consistency_level)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("GetRangeSlices", 1)
	cassandra_ops_latency.Add("GetRangeSlices",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("GetRangeSlices", 1)
	}
	return
}

// returns a range of columns, wrapping to the next rows if necessary to
// collect max_results.
//
// Parameters:
//  - ColumnFamily
//  - RangeA1
//  - StartColumn
//  - ConsistencyLevel
func (self *RetryCassandraClient) GetPagedSlice(column_family string,
	range_a1 *KeyRange, start_column []byte,
	consistency_level ConsistencyLevel) (r []*KeySlice, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.GetPagedSlice(column_family, range_a1, start_column,
		consistency_level)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.GetPagedSlice(column_family, range_a1,
			start_column, consistency_level)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("GetPagedSlice", 1)
	cassandra_ops_latency.Add("GetPagedSlice",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("GetPagedSlice", 1)
	}
	return
}

// Returns the subset of columns specified in SlicePredicate for the rows
// matching the IndexClause
// @deprecated use get_range_slices instead with range.row_filter specified
//
// Parameters:
//  - ColumnParent
//  - IndexClause
//  - ColumnPredicate
//  - ConsistencyLevel
func (self *RetryCassandraClient) GetIndexedSlices(
	column_parent *ColumnParent, index_clause *IndexClause,
	column_predicate *SlicePredicate,
	consistency_level ConsistencyLevel) (r []*KeySlice, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.GetIndexedSlices(column_parent, index_clause,
		column_predicate, consistency_level)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.GetIndexedSlices(column_parent, index_clause,
			column_predicate, consistency_level)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("GetIndexedSlices", 1)
	cassandra_ops_latency.Add("GetIndexedSlices", time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("GetIndexedSlices", 1)
	}
	return
}

// Insert a Column at the given column_parent.column_family and optional
// column_parent.super_column.
//
// Parameters:
//  - Key
//  - ColumnParent
//  - Column
//  - ConsistencyLevel
func (self *RetryCassandraClient) Insert(key []byte,
	column_parent *ColumnParent, column *Column,
	consistency_level ConsistencyLevel) (err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	err = self.wrapped.Insert(key, column_parent, column, consistency_level)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		err = self.wrapped.Insert(key, column_parent, column, consistency_level)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("Insert", 1)
	cassandra_ops_latency.Add("Insert", time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("Insert", 1)
	}
	return
}

// Increment or decrement a counter.
//
// Parameters:
//  - Key
//  - ColumnParent
//  - Column
//  - ConsistencyLevel
func (self *RetryCassandraClient) Add(key []byte,
	column_parent *ColumnParent, column *CounterColumn,
	consistency_level ConsistencyLevel) (err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	err = self.wrapped.Add(key, column_parent, column, consistency_level)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		err = self.wrapped.Add(key, column_parent, column, consistency_level)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("Add", 1)
	cassandra_ops_latency.Add("Add", time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("Add", 1)
	}
	return
}

// Atomic compare and set.
//
// If the cas is successfull, the success boolean in CASResult will be true
// and there will be no current_values.
// Otherwise, success will be false and current_values will contain the
// current values for the columns in expected (that, by definition of
// compare-and-set, will differ from the values in expected).
//
// A cas operation takes 2 consistency level. The first one,
// serial_consistency_level, simply indicates the level of serialization
// required. This can be either ConsistencyLevel.SERIAL or
// ConsistencyLevel.LOCAL_SERIAL.
// The second one, commit_consistency_level, defines the consistency level
// for the commit phase of the cas. This is a more traditional consistency
// level (the same CL than for traditional writes are accepted) that impact
// the visibility for reads of the operation. For instance, if
// commit_consistency_level is QUORUM, then it is guaranteed that a followup
// QUORUM read will see the cas write (if that one was successful obviously).
// If commit_consistency_level is ANY, you will need to use a
// SERIAL/LOCAL_SERIAL read to be guaranteed to see the write.
//
// Parameters:
//  - Key
//  - ColumnFamily
//  - Expected
//  - Updates
//  - SerialConsistencyLevel
//  - CommitConsistencyLevel
func (self *RetryCassandraClient) Cas(key []byte, column_family string,
	expected []*Column, updates []*Column,
	serial_consistency_level ConsistencyLevel,
	commit_consistency_level ConsistencyLevel) (r *CASResult_, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.Cas(key, column_family, expected, updates,
		serial_consistency_level, commit_consistency_level)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.Cas(key, column_family, expected, updates,
			serial_consistency_level, commit_consistency_level)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("Cas", 1)
	cassandra_ops_latency.Add("Cas", time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("Cas", 1)
	}
	return
}

// Remove data from the row specified by key at the granularity specified by
// column_path, and the given timestamp. Note that all the values in
// column_path besides column_path.column_family are truly optional: you can
// remove the entire row by just specifying the ColumnFamily, or you can
// remove a SuperColumn or a single Column by specifying those levels too.
//
// Parameters:
//  - Key
//  - ColumnPath
//  - Timestamp
//  - ConsistencyLevel
func (self *RetryCassandraClient) Remove(key []byte, column_path *ColumnPath,
	timestamp int64, consistency_level ConsistencyLevel) (err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	err = self.wrapped.Remove(key, column_path, timestamp, consistency_level)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		err = self.wrapped.Remove(key, column_path, timestamp,
			consistency_level)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("Remove", 1)
	cassandra_ops_latency.Add("Remove", time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("Remove", 1)
	}
	return
}

// Remove a counter at the specified location.
// Note that counters have limited support for deletes: if you remove a
// counter, you must wait to issue any following update until the delete has
// reached all the nodes and all of them have been fully compacted.
//
// Parameters:
//  - Key
//  - Path
//  - ConsistencyLevel
func (self *RetryCassandraClient) RemoveCounter(key []byte, path *ColumnPath,
	consistency_level ConsistencyLevel) (err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	err = self.wrapped.RemoveCounter(key, path, consistency_level)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		err = self.wrapped.RemoveCounter(key, path, consistency_level)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("RemoveCounter", 1)
	cassandra_ops_latency.Add("RemoveCounter",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("RemoveCounter", 1)
	}
	return
}

// Mutate many columns or super columns for many row keys. See also: Mutation.
//
// mutation_map maps key to column family to a list of Mutation objects to
// take place at that scope.
//
// Parameters:
//  - MutationMap
//  - ConsistencyLevel
func (self *RetryCassandraClient) BatchMutate(
	mutation_map map[string]map[string][]*Mutation,
	consistency_level ConsistencyLevel) (err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	err = self.wrapped.BatchMutate(mutation_map, consistency_level)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		err = self.wrapped.BatchMutate(mutation_map, consistency_level)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("BatchMutate", 1)
	cassandra_ops_latency.Add("BatchMutate",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("BatchMutate", 1)
	}
	return
}

// Atomically mutate many columns or super columns for many row keys. See
// also: Mutation.
//
// mutation_map maps key to column family to a list of Mutation objects to
// take place at that scope.
//
// Parameters:
//  - MutationMap
//  - ConsistencyLevel
func (self *RetryCassandraClient) AtomicBatchMutate(
	mutation_map map[string]map[string][]*Mutation,
	consistency_level ConsistencyLevel) (err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	err = self.wrapped.AtomicBatchMutate(mutation_map, consistency_level)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		err = self.wrapped.AtomicBatchMutate(mutation_map, consistency_level)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("AtomicBatchMutate", 1)
	cassandra_ops_latency.Add("AtomicBatchMutate",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("AtomicBatchMutate", 1)
	}
	return
}

// Truncate will mark and entire column family as deleted.
// From the user's perspective a successful call to truncate will result
// complete data deletion from cfname. Internally, however, disk space will
// not be immediatily released, as with all deletes in cassandra, this one
// only marks the data as deleted.
// The operation succeeds only if all hosts in the cluster at available and
// will throw an UnavailableException if some hosts are down.
//
// Parameters:
//  - Cfname
func (self *RetryCassandraClient) Truncate(cfname string) (err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	err = self.wrapped.Truncate(cfname)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		err = self.wrapped.Truncate(cfname)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("Truncate", 1)
	cassandra_ops_latency.Add("Truncate",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("Truncate", 1)
	}
	return
}

// for each schema version present in the cluster, returns a list of nodes at
// that version. hosts that do not respond will be under the key
// DatabaseDescriptor.INITIAL_VERSION. the cluster is all on the same version
// if the size of the map is 1.
func (self *RetryCassandraClient) DescribeSchemaVersions() (
	r map[string][]string, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.DescribeSchemaVersions()
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.DescribeSchemaVersions()
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("DescribeSchemaVersions", 1)
	cassandra_ops_latency.Add("DescribeSchemaVersions",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("DescribeSchemaVersions", 1)
	}
	return
}

// list the defined keyspaces in this cluster
func (self *RetryCassandraClient) DescribeKeyspaces() (r []*KsDef, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.DescribeKeyspaces()
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.DescribeKeyspaces()
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("DescribeKeyspaces", 1)
	cassandra_ops_latency.Add("DescribeKeyspaces", time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("DescribeKeyspaces", 1)
	}
	return
}

// get the cluster name
func (self *RetryCassandraClient) DescribeClusterName() (
	r string, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.DescribeClusterName()
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.DescribeClusterName()
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("DescribeClusterName", 1)
	cassandra_ops_latency.Add("DescribeClusterName",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("DescribeClusterName", 1)
	}
	return
}

// get the thrift api version
func (self *RetryCassandraClient) DescribeVersion() (r string, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.DescribeVersion()
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.DescribeVersion()
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("DescribeVersion", 1)
	cassandra_ops_latency.Add("DescribeVersion",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("DescribeVersion", 1)
	}
	return
}

// get the token ring: a map of ranges to host addresses,
// represented as a set of TokenRange instead of a map from range
// to list of endpoints, because you can't use Thrift structs as
// map keys:
// https://issues.apache.org/jira/browse/THRIFT-162
//
// for the same reason, we can't return a set here, even though
// order is neither important nor predictable.
//
// Parameters:
//  - Keyspace
func (self *RetryCassandraClient) DescribeRing(keyspace string) (
	r []*TokenRange, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.DescribeRing(keyspace)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.DescribeRing(keyspace)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("DescribeRing", 1)
	cassandra_ops_latency.Add("DescribeRing",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("DescribeRing", 1)
	}
	return
}

// get the mapping between token->node ip
// without taking replication into consideration
// https://issues.apache.org/jira/browse/CASSANDRA-4092
func (self *RetryCassandraClient) DescribeTokenMap() (
	r map[string]string, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.DescribeTokenMap()
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.DescribeTokenMap()
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("DescribeTokenMap", 1)
	cassandra_ops_latency.Add("DescribeTokenMap",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("DescribeTokenMap", 1)
	}
	return
}

// returns the partitioner used by this cluster
func (self *RetryCassandraClient) DescribePartitioner() (r string, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.DescribePartitioner()
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.DescribePartitioner()
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("DescribePartitioner", 1)
	cassandra_ops_latency.Add("DescribePartitioner",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("DescribePartitioner", 1)
	}
	return
}

// returns the snitch used by this cluster
func (self *RetryCassandraClient) DescribeSnitch() (r string, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.DescribeSnitch()
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.DescribeSnitch()
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("DescribeSnitch", 1)
	cassandra_ops_latency.Add("DescribeSnitch",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("DescribeSnitch", 1)
	}
	return
}

// describe specified keyspace
//
// Parameters:
//  - Keyspace
func (self *RetryCassandraClient) DescribeKeyspace(keyspace string) (
	r *KsDef, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.DescribeKeyspace(keyspace)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.DescribeKeyspace(keyspace)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("DescribeKeyspace", 1)
	cassandra_ops_latency.Add("DescribeKeyspace", time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("DescribeKeyspace", 1)
	}
	return
}

// experimental API for hadoop/parallel query support.
// may change violently and without warning.
//
// returns list of token strings such that first subrange is
// (list[0], list[1]], next is (list[1], list[2]], etc.
//
// Parameters:
//  - CfName
//  - StartToken
//  - EndToken
//  - KeysPerSplit
func (self *RetryCassandraClient) DescribeSplits(cfName string,
	start_token string, end_token string, keys_per_split int32) (
	r []string, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.DescribeSplits(cfName, start_token, end_token,
		keys_per_split)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.DescribeSplits(cfName, start_token, end_token,
			keys_per_split)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("DescribeSplits", 1)
	cassandra_ops_latency.Add("DescribeSplits",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("DescribeSplits", 1)
	}
	return
}

// Enables tracing for the next query in this connection and returns the UUID
// for that trace session. The next query will be traced idependently of
// trace probability and the returned UUID can be used to query the trace
// keyspace.
func (self *RetryCassandraClient) TraceNextQuery() (r []byte, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.TraceNextQuery()
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.TraceNextQuery()
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("TraceNextQuery", 1)
	cassandra_ops_latency.Add("TraceNextQuery",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("TraceNextQuery", 1)
	}
	return
}

// Parameters:
//  - CfName
//  - StartToken
//  - EndToken
//  - KeysPerSplit
func (self *RetryCassandraClient) DescribeSplitsEx(cfName string,
	start_token string, end_token string, keys_per_split int32) (
	r []*CfSplit, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.DescribeSplitsEx(cfName, start_token, end_token,
		keys_per_split)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.DescribeSplitsEx(cfName, start_token,
			end_token, keys_per_split)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("DescribeSplitsEx", 1)
	cassandra_ops_latency.Add("DescribeSplitsEx",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("DescribeSplitsEx", 1)
	}
	return
}

// adds a column family. returns the new schema id.
//
// Parameters:
//  - CfDef
func (self *RetryCassandraClient) SystemAddColumnFamily(cf_def *CfDef) (
	r string, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.SystemAddColumnFamily(cf_def)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.SystemAddColumnFamily(cf_def)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("SystemAddColumnFamily", 1)
	cassandra_ops_latency.Add("SystemAddColumnFamily",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("SystemAddColumnFamily", 1)
	}
	return
}

// drops a column family. returns the new schema id.
//
// Parameters:
//  - ColumnFamily
func (self *RetryCassandraClient) SystemDropColumnFamily(
	column_family string) (r string, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.SystemDropColumnFamily(column_family)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.SystemDropColumnFamily(column_family)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("SystemDropColumnFamily", 1)
	cassandra_ops_latency.Add("SystemDropColumnFamily",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("SystemDropColumnFamily", 1)
	}
	return
}

// adds a keyspace and any column families that are part of it. returns the
// new schema id.
//
// Parameters:
//  - KsDef
func (self *RetryCassandraClient) SystemAddKeyspace(ks_def *KsDef) (
	r string, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.SystemAddKeyspace(ks_def)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.SystemAddKeyspace(ks_def)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("SystemDropColumnFamily", 1)
	cassandra_ops_latency.Add("SystemDropColumnFamily",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("SystemDropColumnFamily", 1)
	}
	return
}

// drops a keyspace and any column families that are part of it. returns the
// new schema id.
//
// Parameters:
//  - Keyspace
func (self *RetryCassandraClient) SystemDropKeyspace(
	keyspace string) (r string, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.SystemDropKeyspace(keyspace)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.SystemDropKeyspace(keyspace)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("SystemDropKeyspace", 1)
	cassandra_ops_latency.Add("SystemDropKeyspace",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("SystemDropKeyspace", 1)
	}
	return
}

// updates properties of a keyspace. returns the new schema id.
//
// Parameters:
//  - KsDef
func (self *RetryCassandraClient) SystemUpdateKeyspace(ks_def *KsDef) (
	r string, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.SystemUpdateKeyspace(ks_def)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.SystemUpdateKeyspace(ks_def)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("SystemUpdateKeyspace", 1)
	cassandra_ops_latency.Add("SystemUpdateKeyspace",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("SystemUpdateKeyspace", 1)
	}
	return
}

// updates properties of a column family. returns the new schema id.
//
// Parameters:
//  - CfDef
func (self *RetryCassandraClient) SystemUpdateColumnFamily(cf_def *CfDef) (
	r string, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.SystemUpdateColumnFamily(cf_def)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.SystemUpdateColumnFamily(cf_def)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("SystemUpdateColumnFamily", 1)
	cassandra_ops_latency.Add("SystemUpdateColumnFamily",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("SystemUpdateColumnFamily", 1)
	}
	return
}

// @deprecated Will become a no-op in 2.2. Please use the CQL3 version instead.
//
// Parameters:
//  - Query
//  - Compression
func (self *RetryCassandraClient) ExecuteCqlQuery(query []byte,
	compression Compression) (r *CqlResult_, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.ExecuteCqlQuery(query, compression)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.ExecuteCqlQuery(query, compression)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("ExecuteCqlQuery", 1)
	cassandra_ops_latency.Add("ExecuteCqlQuery",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("ExecuteCqlQuery", 1)
	}
	return
}

// Executes a CQL3 (Cassandra Query Language) statement and returns a
// CqlResult containing the results.
//
// Parameters:
//  - Query
//  - Compression
//  - Consistency
func (self *RetryCassandraClient) ExecuteCql3Query(query []byte,
	compression Compression, consistency ConsistencyLevel) (
	r *CqlResult_, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.ExecuteCql3Query(query, compression, consistency)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.ExecuteCql3Query(query, compression, consistency)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("ExecuteCql3Query", 1)
	cassandra_ops_latency.Add("ExecuteCql3Query",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("ExecuteCql3Query", 1)
	}
	return
}

// @deprecated Will become a no-op in 2.2. Please use the CQL3 version instead.
//
// Parameters:
//  - Query
//  - Compression
func (self *RetryCassandraClient) PrepareCqlQuery(query []byte,
	compression Compression) (r *CqlPreparedResult_, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.PrepareCqlQuery(query, compression)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.PrepareCqlQuery(query, compression)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("PrepareCqlQuery", 1)
	cassandra_ops_latency.Add("PrepareCqlQuery",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("PrepareCqlQuery", 1)
	}
	return
}

// Prepare a CQL3 (Cassandra Query Language) statement by compiling and
// returning
// - the type of CQL statement
// - an id token of the compiled CQL stored on the server side.
// - a count of the discovered bound markers in the statement
//
// Parameters:
//  - Query
//  - Compression
func (self *RetryCassandraClient) PrepareCql3Query(query []byte,
	compression Compression) (r *CqlPreparedResult_, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.PrepareCql3Query(query, compression)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.PrepareCql3Query(query, compression)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("PrepareCql3Query", 1)
	cassandra_ops_latency.Add("PrepareCql3Query",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("PrepareCql3Query", 1)
	}
	return
}

// @deprecated Will become a no-op in 2.2. Please use the CQL3 version instead.
//
// Parameters:
//  - ItemId
//  - Values
func (self *RetryCassandraClient) ExecutePreparedCqlQuery(itemId int32,
	values [][]byte) (r *CqlResult_, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.ExecutePreparedCqlQuery(itemId, values)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.ExecutePreparedCqlQuery(itemId, values)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("ExecutePreparedCqlQuery", 1)
	cassandra_ops_latency.Add("ExecutePreparedCqlQuery",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("ExecutePreparedCqlQuery", 1)
	}
	return
}

// Executes a prepared CQL3 (Cassandra Query Language) statement by passing
// an id token, a list of variables to bind, and the consistency level, and
// returns a CqlResult containing the results.
//
// Parameters:
//  - ItemId
//  - Values
//  - Consistency
func (self *RetryCassandraClient) ExecutePreparedCql3Query(itemId int32,
	values [][]byte, consistency ConsistencyLevel) (r *CqlResult_, err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	r, err = self.wrapped.ExecutePreparedCql3Query(itemId, values, consistency)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		r, err = self.wrapped.ExecutePreparedCql3Query(itemId, values,
			consistency)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("ExecutePreparedCqlQuery", 1)
	cassandra_ops_latency.Add("ExecutePreparedCqlQuery",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("ExecutePreparedCqlQuery", 1)
	}
	return
}

// @deprecated This is now a no-op. Please use the CQL3 specific methods
// instead.
//
// Parameters:
//  - Version
func (self *RetryCassandraClient) SetCqlVersion(version string) (err error) {
	var begin time.Time = time.Now()
	self.mtx.RLock()
	err = self.wrapped.SetCqlVersion(version)
	if IsRetryable(err) {
		self.mtx.RUnlock()
		self.Reconnect()
		begin = time.Now()
		self.mtx.RLock()
		err = self.wrapped.SetCqlVersion(version)
	}
	self.mtx.RUnlock()
	cassandra_num_ops.Add("SetCqlVersion", 1)
	cassandra_ops_latency.Add("SetCqlVersion",
		time.Now().UnixNano()-begin.UnixNano())
	if err != nil {
		cassandra_num_errors.Add("SetCqlVersion", 1)
	}
	return
}
