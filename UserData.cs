namespace Gazmon
{
    internal class UserData
    {
        public string aws_access_key_id { get; }
        public string aws_secret_access_key { get; }

        public UserData(string aws_access_key_id, string aws_secret_access_key)
        {
            this.aws_access_key_id = aws_access_key_id;
            this.aws_secret_access_key = aws_secret_access_key;

        }
    }
}
    