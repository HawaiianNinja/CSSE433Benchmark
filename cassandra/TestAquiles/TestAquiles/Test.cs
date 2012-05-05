using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using Aquiles.Cassandra10;
using Aquiles.Core.Cluster;
using Aquiles.Helpers;
using Aquiles.Helpers.Encoders;

namespace TestAquiles
{
    internal class Test
    {
//        private string columnFamily = "users";
//        private string keyspace = "test";
//        private string key = "testKey";
//        // Insert statement
////        private byte[] key = ByteEncoderHelper.LongEncoder.ToByteArray(i);
//        private byte[] key = ByteEncoderHelper.LongEncoder.ToByteArray(2);
//        private ColumnParent columnParent;
//        private Column column;
//        private ColumnPath columnPath;
//        private ICluster cluster;

//        
//
//
//cluster.Execute(new ExecutionBlock(delegate(CassandraClient client) {
//    client.insert(key, columnParent, column, ConsistencyLevel.ONE);
//    return null;
//}), keyspace);

//        // Get statement
//        byte[] key = ByteEncoderHelper.LongEncoder.ToByteArray(2);
//        
//        ICluster cluster = AquilesHelper.RetrieveCluster("Cassandra1");
//        object rtnValue = cluster.Execute(new ExecutionBlock(delegate(CassandraClient client)
//        {
//            return client.get(key, columnPath, ConsistencyLevel.ONE);
//        }), keyspace);

        public void Run()
        {
            string columnFamily = "TestColumnFamily";
            string keyspace = "TestKeyspace";
            string key = "testKey";
            string columnName = "testColumn";
            string columnValue = "testValue";

            // Insert statement
            byte[] key2 = ByteEncoderHelper.LongEncoder.ToByteArray(1);
            ColumnParent columnParent = new ColumnParent();
            Column column = new Column()
            {
                Name = ByteEncoderHelper.UTF8Encoder.ToByteArray(columnName),
                Timestamp = UnixHelper.UnixTimestamp,
                Value = ByteEncoderHelper.UTF8Encoder.ToByteArray(columnValue),
            };

            columnParent.Column_family = columnFamily;

            ICluster cluster = AquilesHelper.RetrieveCluster("Cassandra1");
            cluster.Execute(new ExecutionBlock(delegate(CassandraClient client)
            {
                client.insert(key, columnParent, column, ConsistencyLevel.ONE);
                return null;
            }), keyspace);
        }
    }
}