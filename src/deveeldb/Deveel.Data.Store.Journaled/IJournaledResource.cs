using System;
using System.IO;

namespace Deveel.Data.Store.Journaled {
	/// <summary>
	/// A resource that is handled by a journaling system.
	/// </summary>
	public interface IJournaledResource : IDisposable {
		/// <summary>
		/// Gets the page size of the resource.
		/// </summary>
		int PageSize { get; }

		/// <summary>
		/// Gets a unique id for this resource.
		/// </summary>
		long Id { get; }

		/// <summary>
		/// Gets the size of the resource within the system.
		/// </summary>
		long Size { get; }

		/// <summary>
		/// Gets a value indicating whether this instance is read only.
		/// </summary>
		/// <value>
		/// <c>true</c> if this instance is read only; otherwise, <c>false</c>.
		/// </value>
		bool IsReadOnly { get; }

		/// <summary>
		/// Gets a value indicating whether this <see cref="IJournaledResource"/> exists
		/// in the underlying system.
		/// </summary>
		/// <value>
		///   <c>true</c> if exists; otherwise, <c>false</c>.
		/// </value>
		bool Exists { get; }


		/// <summary>
		/// Reads the specified page number into the given buffer.
		/// </summary>
		/// <param name="pageNumber">The page number to read.</param>
		/// <param name="buffer">The buffer where to copy the data read.</param>
		/// <param name="offset">The offset within the buffer where to start
		/// copying the data read.</param>
		void Read(long pageNumber, byte[] buffer, int offset);

		/// <summary>
		/// Writes the given data at the specified page number.
		/// </summary>
		/// <param name="pageNumber">The page number where to write the data.</param>
		/// <param name="buffer">The data to write to the page.</param>
		/// <param name="offset">The offset withing the buffer giver where to start
		/// extracting the data to write.</param>
		/// <param name="length">The length of data to write.</param>
		/// <exception cref="IOException">
		/// If the 
		/// </exception>
		void Write(long pageNumber, byte[] buffer, int offset, int length);

		/// <summary>
		/// Sets a new size of the resources.
		/// </summary>
		/// <param name="size">The new size to set.</param>
		/// <seealso cref="Size"/>
		void SetSize(long size);

		/// <summary>
		/// Opens the resource for usage with a given read-only mode.
		/// </summary>
		/// <param name="readOnly">If set to <c>true</c> the resource is opened
		/// in read-only mode: any attempt to modify it will throw an exception.</param>
		void Open(bool readOnly);

		/// <summary>
		/// Closes this instance and prevents any further access to the
		/// underlying data.
		/// </summary>
		void Close();

		/// <summary>
		/// Deletes this instance from the underlying system.
		/// </summary>
		void Delete();
	}
}
