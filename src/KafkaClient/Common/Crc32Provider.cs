// Copyright (c) Damien Guard.  All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Originally published at http://damieng.com/blog/2006/08/08/calculating_crc32_in_c_and_net

using System;
using System.Collections.Generic;
using System.Linq;

namespace KafkaClient.Common
{
    /// <summary>
    /// This code was originally from the copyrighted code listed above but was modified significantly
    /// as the original code was not thread safe and did not match was was required of this driver. This
    /// class now provides a static lib which will do the simple CRC calculation required by Kafka servers.
    /// </summary>
    public static class Crc32Provider
    {
        public const uint DefaultPolynomial = 0xedb88320u;
        public const uint DefaultSeed = 0xffffffffu;
        private static readonly uint[] PolynomialTable;

        static Crc32Provider()
        {
            PolynomialTable = InitializeTable(DefaultPolynomial);
        }

        public static uint Compute(IEnumerable<byte> buffer)
        {
            return ~CalculateHash(buffer);
        }

        public static uint Compute(byte[] buffer)
        {
            return ~CalculateHash(buffer, 0, buffer.Length);
        }

        public static uint Compute(byte[] buffer, int offset, int length)
        {
            return ~CalculateHash(buffer, offset, length);
        }

        public static byte[] ComputeHash(IEnumerable<byte> buffer)
        {
            return UInt32ToBigEndianBytes(Compute(buffer));
        }

        public static byte[] ComputeHash(byte[] buffer)
        {
            return UInt32ToBigEndianBytes(Compute(buffer));
        }

        public static byte[] ComputeHash(byte[] buffer, int offset, int length)
        {
            return UInt32ToBigEndianBytes(Compute(buffer, offset, length));
        }

        private static uint[] InitializeTable(uint polynomial)
        {
            var createTable = new uint[256];
            for (var i = 0; i < 256; i++) {
                var entry = (uint)i;
                for (var j = 0; j < 8; j++) {
                    if ((entry & 1) == 1) {
                        entry = (entry >> 1) ^ polynomial;
                    } else {
                        entry = entry >> 1;
                    }
                }
                createTable[i] = entry;
            }

            return createTable;
        }

        private static uint CalculateHash(byte[] buffer, int offset, int length)
        {
            var crc = DefaultSeed;
            for (var i = offset; i < length; i++) {
                crc = (crc >> 8) ^ PolynomialTable[buffer[i] ^ crc & 0xff];
            }
            return crc;
        }

        private static uint CalculateHash(IEnumerable<byte> buffer)
        {
            return buffer.Aggregate(DefaultSeed, (crc, value) => (crc >> 8) ^ PolynomialTable[value ^ crc & 0xff]);
        }

        private static byte[] UInt32ToBigEndianBytes(uint uint32)
        {
            var result = BitConverter.GetBytes(uint32);

            if (BitConverter.IsLittleEndian) {
                Array.Reverse(result);
            }

            return result;
        }
    }
}