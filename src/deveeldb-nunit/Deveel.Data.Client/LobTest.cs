﻿using System;
using System.Data;
using System.IO;

using NUnit.Framework;

namespace Deveel.Data.Client {
	[TestFixture]
	public sealed class LobTest : TestBase {
		protected override void OnTestSetUp() {
			DeveelDbCommand command = Connection.CreateCommand("CREATE TABLE IF NOT EXISTS LOB_TEST (Id INTEGER NOT NULL, Data BLOB)");
			command.ExecuteNonQuery();
		}

		protected override void OnTestTearDown() {
			if (Connection.State == ConnectionState.Open) {
				DeveelDbCommand command = Connection.CreateCommand("DROP TABLE IF EXISTS LOB_TEST");
				command.ExecuteNonQuery();
			}
		}

		[Test]
		public void SmallBlobWrite() {
			/*
			TODO: The behavior of LOBs and Binaries has changed ...
			DeveelDbCommand command = Connection.CreateCommand("INSERT INTO LOB_TEST (Id, Data) VALUES (1, ?)");
			DeveelDbLargeObject lob = new DeveelDbLargeObject(ReferenceType.Binary, 1024 * 4);

			BinaryWriter writer = new BinaryWriter(lob);
			Random rnd = new Random();
			for (int i = 1; i <= 1024; i++) {
				int value = rnd.Next();
				writer.Write(value);
			}

			writer.Flush();

			// the parameter can be added in every moment: there is not a required
			// order to do it ...
			command.Parameters.Add(lob);
			command.Prepare();

			int count = command.ExecuteNonQuery();

			Assert.AreEqual(1, count);
			*/

			Assert.Ignore("Changed behavior in binary data handling: waiting for better integration.");
		}

		[Test]
		public void SmallBlobRead() {
			/*
			TODO: The behavior of LOBs and Binaries has changed ...
			SmallBlobWrite();

			DeveelDbCommand command = Connection.CreateCommand("SELECT Data FROM LOB_TEST WHERE Id = 1");
			DeveelDbDataReader reader = command.ExecuteReader();
			if (reader.Read()) {
				DeveelDbLargeObject lob = reader.GetLargeObject(0);
				byte[] buffer = new byte[1024];
				int offset = 0;
				long length = lob.Length;
				while (offset < length) {
					int readCount = lob.Read(buffer, 0, buffer.Length);

					// handle the buffer in some way...

					offset += readCount;
				}
			}
			*/

			Assert.Ignore("Changed behavior in binary data handling: waiting for better integration.");
		}
	}
}