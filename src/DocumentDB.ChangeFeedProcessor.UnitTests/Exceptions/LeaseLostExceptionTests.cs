//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
using Microsoft.Azure.Documents.ChangeFeedProcessor.LeaseManagement;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
using Moq;
using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Xunit;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.Exceptions
{
    [Trait("Category", "Gated")]
    public class LeaseLostExceptionTests
    {
        [Fact]
        public void ValidateRecommendedConstructors()
        {
            // Default ctor.
            var ex = new LeaseLostException();
            Assert.NotNull(ex.Message);

            // ctor(message).
            string message = "message";
            ex = new LeaseLostException(message);
            Assert.Equal(message, ex.Message);

            // ctor()
            Exception innerException = new Exception();
            ex = new LeaseLostException(message, innerException);
            Assert.Equal(message, ex.Message);
            Assert.Equal(innerException, ex.InnerException);
        }

        [Fact]
        public void ValidateLeaseContructor()
        {
            var lease = Mock.Of<ILease>();
            var ex = new LeaseLostException(lease);
            Assert.Equal(lease, ex.Lease);
            Assert.NotNull(ex.Message);
        }

        [Fact]
        public void ValidateIsGoneConstructor()
        {
            var lease = Mock.Of<ILease>();
            var innerException = new Exception();
            var ex = new LeaseLostException(lease, innerException, true);

            Assert.NotNull(ex.Message);
            Assert.Equal(lease, ex.Lease);
            Assert.Equal(innerException, ex.InnerException);
            Assert.True(ex.IsGone);
        }

        // Tests the GetObjectData method and the serialization ctor.
        [Fact]
        public void ValidateSerialization_AllFields()
        {
            var lease = new DocumentServiceLease { Id = "id" };
            var originalException = new LeaseLostException(lease, new Exception("foo"), true);
            var buffer = new byte[4096];
            var formatter = new BinaryFormatter();
            var stream1 = new MemoryStream(buffer);
            var stream2 = new MemoryStream(buffer);

            formatter.Serialize(stream1, originalException);
            var deserializedException = (LeaseLostException)formatter.Deserialize(stream2);

            Assert.Equal(originalException.Message, deserializedException.Message);
            Assert.Equal(originalException.InnerException.Message, deserializedException.InnerException.Message);
            Assert.Equal(originalException.Lease.Id, deserializedException.Lease.Id);
            Assert.Equal(originalException.IsGone, deserializedException.IsGone);
        }

        // Make sure that when some fields are not set, serialization is not broken.
        [Fact]
        public void ValidateSerialization_NullFields()
        {
            var originalException = new LeaseLostException("message");
            var buffer = new byte[4096];
            var formatter = new BinaryFormatter();
            var stream1 = new MemoryStream(buffer);
            var stream2 = new MemoryStream(buffer);

            formatter.Serialize(stream1, originalException);
            var deserializedException = (LeaseLostException)formatter.Deserialize(stream2);

            Assert.Equal(originalException.Message, deserializedException.Message);
            Assert.Null(deserializedException.InnerException);
            Assert.Null(deserializedException.Lease);
            Assert.False(deserializedException.IsGone);
        }
    }
}
