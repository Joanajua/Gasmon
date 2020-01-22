namespace Gazmon
{
    internal class JsonFile
    {
        public string x { get; set; }
        public string y { get; set; }
        public string id { get; set; }

        
        public string CuttingCommaX ()
        {
            this.x = x.Remove(',');
            return x;
           
        }
        public void CuttingCommaY(string valueAtribute)
        {
            string jsonAtribute = y.TrimEnd(',');

        }

    }
}