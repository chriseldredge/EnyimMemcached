using System;
using System.Text;

namespace Enyim.Caching.Memcached.Protocol.Binary
{
	public class BinaryResponse
	{
		private static readonly Enyim.Caching.ILog log = Enyim.Caching.LogManager.GetLogger(typeof(BinaryResponse));

		private const byte MAGIC_VALUE = 0x81;
		private const int HEADER_OPCODE = 1;
		private const int HEADER_KEY = 2; // 2-3
		private const int HEADER_EXTRA = 4;
		private const int HEADER_DATATYPE = 5;
		private const int HEADER_STATUS = 6; // 6-7
		private const int HEADER_BODY = 8; // 8-11
		private const int HEADER_OPAQUE = 12; // 12-15
		private const int HEADER_CAS = 16; // 16-23

		public byte Opcode;
		public int KeyLength;
		public byte DataType;
		public int StatusCode;

		public int CorrelationId;
		public ulong CAS;

		public ArraySegment<byte> Extra;
		public ArraySegment<byte> Data;

		private string responseMessage;

		public string GetStatusMessage()
		{
			return this.Data.Array == null
					? null
					: (this.responseMessage
						?? (this.responseMessage = Encoding.ASCII.GetString(this.Data.Array, this.Data.Offset, this.Data.Count)));
		}

		public bool Read(PooledSocket socket)
		{
			if (!socket.IsAlive)
			{
				this.StatusCode = -1;
				return false;
			}
			
			var header = new byte[24];
			socket.Read(header, 0, 24);

			var data = DecodeHeader(header);

			socket.Read(data, 0, data.Length);

			return this.StatusCode == 0;
		}

		public IAsyncResult BeginRead(PooledSocket socket, AsyncCallback callback, object state)
		{
			var asyncResult = new AsyncResult<bool>(state);

			if (!socket.IsAlive)
			{
				this.StatusCode = -1;
				asyncResult.SetComplete(false);
				return asyncResult;
			}

			var header = new byte[24];

			AsyncCallback endReadContent =
				callbackResult =>
					{
						try
						{
							socket.EndRead(callbackResult);

							asyncResult.SetComplete(this.StatusCode == 0);
						}
						catch (Exception e)
						{
							log.Error(e.Message, e);
							asyncResult.SetComplete(false);
						}

						if (callback != null)
						{
							callback(asyncResult);
						}
					};

			AsyncCallback endReadHeader =
				ar =>
					{
						try
						{
							socket.EndRead(ar);

							var data = DecodeHeader(header);

							if (data.Length > 0)
							{
								log.Debug("BinaryResponse.BeginRead.endReadHeader: read " + data.Length + " more bytes.");

								socket.BeginRead(data, 0, data.Length, endReadContent, null);

								return;
							}

							asyncResult.SetComplete(true);
						}
						catch (Exception e)
						{
							log.Error("BinaryResponse.BeginRead.endReadHeader", e);
							asyncResult.SetComplete(false);
						}
						if (callback != null)
						{
							callback(asyncResult);
						}

					};

			log.Debug("BinaryResponse.BeginRead: read header.");
			socket.BeginRead(header, 0, 24, endReadHeader, null);

			return asyncResult;
		}

		public bool EndRead(IAsyncResult ar)
		{
			return this.StatusCode == 0;
		}

		private unsafe byte[] DecodeHeader(byte[] header)
		{
#if DEBUG_PROTOCOL
			if (log.IsDebugEnabled)
			{
				log.Debug("Received binary response");

				StringBuilder sb = new StringBuilder(128).AppendLine();

				for (int i = 0; i < header.Length; i++)
				{
					byte value = header[i];
					sb.Append(value < 16 ? "0x0" : "0x").Append(value.ToString("X"));

					if (i % 4 == 3) sb.AppendLine(); else sb.Append(" ");
				}

				log.Debug(sb.ToString());
			}
#endif

			fixed (byte* buffer = header)
			{
				if (buffer[0] != MAGIC_VALUE)
					throw new InvalidOperationException("Expected magic value " + MAGIC_VALUE + ", received: " + buffer[0]);

				this.DataType = buffer[HEADER_DATATYPE];
				this.Opcode = buffer[HEADER_OPCODE];
				this.StatusCode = BinaryConverter.DecodeInt16(buffer, HEADER_STATUS);

				this.KeyLength = BinaryConverter.DecodeInt16(buffer, HEADER_KEY);
				this.CorrelationId = BinaryConverter.DecodeInt32(buffer, HEADER_OPAQUE);
				this.CAS = BinaryConverter.DecodeUInt64(buffer, HEADER_CAS);

				int remaining = BinaryConverter.DecodeInt32(buffer, HEADER_BODY);
				int extraLength = buffer[HEADER_EXTRA];

				var data = new byte[remaining];

				this.Extra = new ArraySegment<byte>(data, 0, extraLength);
				this.Data = new ArraySegment<byte>(data, extraLength, data.Length - extraLength);

				return data;
			}
		}
	}

#region [ License information		  ]
/* ************************************************************
 * 
 *    Copyright (c) 2010 Attila Kisk�, enyim.com
 *    
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *    
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *    
 * ************************************************************/
#endregion
