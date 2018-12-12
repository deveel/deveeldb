using System;

using Deveel.Collections;
using Deveel.Data.Storage;

namespace Deveel.Data.Sql.Tables
{
	interface IMappedBlock : ICollectionBlock<SqlObject, long> {
		long FirstEntry { get; }

		long LastEntry { get; }

		long BlockPointer { get; }

		byte CompactType { get; }


		long CopyTo(IStore destStore);

		long Flush();
	}}
