using System;
using System.IO;

namespace Deveel.Data.Store.Journaled {
	interface ISystemJournaledResource : IJournaledResource {
		void PersistDelete();

		void PersistClose();

		void PersistSetSize(long newSize);

		void PersistPageChange(long page, int offset, int length, BinaryReader source);

		void Synch();

		void OnRecovered();
	}
}
