namespace StronglyTypedBlobStorage {
    using System.IO;
    using System.Threading.Tasks;

    public interface ISerializer<T>
    {
        Task<T> Deserialize(Stream fromStream);
        Stream Serialize(T item);
    }
}