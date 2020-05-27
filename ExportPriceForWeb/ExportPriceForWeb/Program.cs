using FirebirdSql.Data.FirebirdClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ExportPriceForWeb
{
    class Program
    {
        static FbConnection fb; //fb ссылается на соединение с нашей базой данных, поэтому она должна быть доступна всем методам нашего класса
        public static string path_db;
        public static string FileName;

        static void Main(string[] args)
        {
            LicensyaCheck();

            Load();

            //Console.ReadLine();
        }

        private static void Load()
        {
            try
            {
                string path = System.Reflection.Assembly.GetExecutingAssembly().Location;
                var directory = Path.GetDirectoryName(path);

                //Создание объекта, для работы с файлом
                INIManager manager = new INIManager(directory + @"\set.ini");
                //Получить значение по ключу name из секции main
                path_db = manager.GetPrivateString("connection", "db");


                File.AppendAllText(directory + @"\Event.log", "путь к db:" + path_db + "\n");
                //Записать значение по ключу age в секции main
                // manager.WritePrivateString("main", "age", "21");

            }
            catch (Exception ex)
            {
                Console.WriteLine("ini не прочтен: " + ex.Message);
            }

            int res = 0;
            fb = GetConnection();
            try
            {
                PriceTabel(fb, null, null, null, null);

                FileName = $"Report_00_{DateTime.Now.ToString("dd-MM-yyyy")}.csv";

                if (!string.IsNullOrEmpty(FileName))
                {
                    res = WorkWithReport2(FileName);
                }

                // }
                if (res != 0) Console.WriteLine($"Выгрузка успешно {res}");
                else Console.WriteLine($"Выгрузка отмена {res}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Выгрузка неудачно {ex.Message}");
            }
        }

        private static int WorkWithReport2(string fileName)
        {
            int count = 0;

            using (var w = new StreamWriter(new FileStream(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite), Encoding.UTF8))
            // using (var w = new StreamWriter(textBox2.Text))
            {
                foreach (DataColumn column in PriceTabel(fb, null, null, null, null).Columns)
                {
                    if (column.Ordinal < 11) w.Write($"{column.ColumnName};");
                    else w.Write($"{column.ColumnName}");
                    count++;
                }
                w.Write("\n");

                foreach (DataRow dataRow in PriceTabel(fb, null, null, null, null).AsEnumerable().ToList())
                {
                    var idGroup = dataRow[0].ToString(); // 1
                    var nameGroup = dataRow[1].ToString(); // 2
                    var idGoods = dataRow[2].ToString(); // 3
                    var nameGoods = dataRow[3].ToString(); // 4
                    var nameGoodsRus = dataRow[4].ToString(); // 4
                    var typeGoods = dataRow[5].ToString(); // 5
                    var priceGoods = dataRow[6].ToString(); // 6
                    var idSost = dataRow[7].ToString(); // 7
                    var codeGoods = dataRow[8].ToString(); // 8
                    var DESCR = dataRow[9].ToString(); // 9
                    var DAYS_FROM_GET = dataRow[10].ToString(); // 10
                    var BULB_NUM = dataRow[11].ToString(); // 11
                    var MATERIAL = dataRow[12].ToString(); // 12

                    var line = string.Format($"{idGroup};{nameGroup};{idGoods};{nameGoods};{nameGoodsRus};{typeGoods};{priceGoods};{idSost};{codeGoods};{DESCR};{DAYS_FROM_GET};{BULB_NUM};{MATERIAL}");
                    w.WriteLine(line);
                    w.Flush();
                    count++;

                    Console.WriteLine(line);
                }

            }
            return count;
        }

        public static DataTable PriceTabel(FbConnection conn, string GRP_ID, string FILTER_, string REFRESH_ID, string IN_ORG_ID)
        {
            if (string.IsNullOrEmpty(GRP_ID)) { GRP_ID = "null"; }
            if (string.IsNullOrEmpty(FILTER_)) { FILTER_ = "null"; }
            if (string.IsNullOrEmpty(REFRESH_ID)) { REFRESH_ID = "null"; }
            if (string.IsNullOrEmpty(IN_ORG_ID)) { IN_ORG_ID = "null"; }


            string query = "select  dgg.ID as ID_GROUP /*id группы*/ " +
                        ", dgg.name as NAME_GROUP   /*имя группы*/ " +
                        ",dg.ID as ID_GOODS      /*id goods*/ " +
                        ",dg.NAME as NAME_GOODS    /*имя услуги*/ " +
                        ",case when(dt.val_text is null) then '' else dt.val_text end as NAME_GOODS_RUS " +
                        ",case  /*IS_NORMS_EXISTS and IS_CALCS_EXISTS*/ " +
                        "when not exists(select first 1 1 " +
                        "from DIC_GOODS_LAB_NORMS N " +
                        "where N.GOODS_ID = dg.ID) and " +
                        "exists(select first 1 1 " +
                        "from DIC_CALCULATIONS C " +
                        "where C.HD_ID = DG.ID) then 1 " +
                        "when exists(select first 1 1 " +
                        "from DIC_GOODS_LAB_NORMS N " +
                        "where N.GOODS_ID = dg.ID) and " +
                        "exists(select first 1 1 " +
                        "from DIC_CALCULATIONS C " +
                        "where C.HD_ID = DG.ID) " +
                        "and dg.id = '65' then 1 " +
                        "else 0 " +
                        "end as TYPE_GOODS " +
                        ",case when(dg.PRICE_OUT is null) then 0 else dg.PRICE_OUT end as PRICE_GOODS " +
                        ",0 as ID_SOST " +
                        ",case when(dg.code is null) then 0 else dg.code end as CODE_GOODS " +
                        ",case when(dg.descr is null) then '' else dg.descr end as DESCR " +
                        ",case when(dg.DAYS_FROM_GET is null) then 0 else dg.DAYS_FROM_GET end as DAYS_FROM_GET " +
                        ",case when(dbt.name is null) then '' else dbt.name end as BULB_NUM " +
                        ",case when(dd.val is null) then '' else dd.val end as MATERIAL " +
                        "from dic_goods dg " +
                        "join DIC_GOODS_GRP dgg on dg.GRP_ID = dgg.ID " +
                        "left join DIC_BULB_TYPES dbt on dbt.id = dg.bulb_num_id " +
                        "left join DIC_DICS dd on dd.id = dg.material_id " +
                        "left join dic_translations dt on dt.key_ like 'DIC_GOODS_NAME_%' and dt.key_ like '%_' || dg.id and dt.lng_id = '9f8eb4687e544e33a7723c46547a6b00' " +
                        "where DG.IS_SERVICE = 1 and DG.IS_ACTIVE = 1 and dgg.id <> '189' " +
                        "union " +
                        "/*part 2*/ " +
                        "select dgg1.ID as ID_GROUP      /*id группы*/ " +
                        ",dgg1.name as NAME_GROUP    /*имя группы*/ " +
                        ",dg1.ID as ID_GOODS       /*id goods*/ " +
                        ",dg1.NAME as NAME_GOODS     /*имя услуги*/ " +
                        ",case when(dt1.val_text is null) then '' else dt1.val_text end as NAME_GOODS_RUS "+
                        ",case  /*IS_NORMS_EXISTS and IS_CALCS_EXISTS*/ " +
                        "when not exists(select first 1 1 " +
                        "from DIC_GOODS_LAB_NORMS N " +
                        "where N.GOODS_ID = dg.ID) and " +
                        "exists(select first 1 1 " +
                        "from DIC_CALCULATIONS C " +
                        "where C.HD_ID = dg.ID) then 2 " +
                        "when exists(select first 1 1 " +
                        "from DIC_GOODS_LAB_NORMS N " +
                        "where N.GOODS_ID = dg.ID) and " +
                        "exists(select first 1 1 " +
                        "from DIC_CALCULATIONS C " +
                        "where C.HD_ID = DG.ID) " +
                        "and dg.id = '65' then 2 " +
                        "else 5 " +
                        "end as TYPE_GOODS " +
                        ",case when(dg1.PRICE_OUT is null) then 0 else dg1.PRICE_OUT end as PRICE_GOODS " +
                        ",case  /*IS_NORMS_EXISTS and IS_CALCS_EXISTS*/ " +
                        "when not exists(select first 1 1 " +
                        "from DIC_GOODS_LAB_NORMS N " +
                        "where N.GOODS_ID = dg.ID) " +
                        "and exists(select first 1 1 " +
                        "from DIC_CALCULATIONS C " +
                        "where C.HD_ID = dg.ID) then dg.ID " +
                        "when exists(select first 1 1 " +
                        "from DIC_GOODS_LAB_NORMS N " +
                        "where N.GOODS_ID = dg.ID) and " +
                        "exists(select first 1 1 " +
                        "from DIC_CALCULATIONS C " +
                        "where C.HD_ID = DG.ID) " +
                        "and dg.id = '65' then DG.ID " +
                        "else 0 " +
                        "end as ID_SOST " +
                        ",case when(dg1.code is null) then 0 else dg1.code end as CODE_GOODS " +
                        ",case when(dg1.descr is null) then '' else dg1.descr end as DESCR " +
                        ",case when(dg1.DAYS_FROM_GET is null) then 0 else dg1.DAYS_FROM_GET end as DAYS_FROM_GET " +
                        ",case when(dbt.name is null) then '' else dbt.name end as BULB_NUM " +
                        ",case when(dd.val is null) then '' else dd.val end as MATERIAL " +
                        "from dic_goods dg " +
                        "join DIC_GOODS_GRP dgg on dg.GRP_ID = dgg.ID " +
                        "left " +
                        "join DIC_CALCULATIONS dc on dc.hd_id = dg.id and dc.IS_AUTO_ADD = 0 " +
                        "left join dic_goods dg1 on dg1.id = dc.goods_id " +
                        "left join DIC_GOODS_GRP dgg1 on dg1.GRP_ID = dgg1.ID " +
                        "left join DIC_BULB_TYPES dbt on dbt.id = dg1.bulb_num_id " +
                        "left join DIC_DICS dd on dd.id = dg1.material_id " +
                        "left join dic_translations dt1 on dt1.key_ like 'DIC_GOODS_NAME_%' and dt1.key_ like '%_' || dg.id and dt1.lng_id = '9f8eb4687e544e33a7723c46547a6b00' " +
                        "where DG.IS_SERVICE = 1 and DG.IS_ACTIVE = 1 and dgg.id <> '189' " +
                        "and((case " +
                        "when dc.IS_AUTO_ADD = 1 then(select RESULT " +
                        "                  from translate('IS_AUTO_ADD')) " +
                        "when((dg1.IS_SERVICE = 1) and(dc.IS_AUTO_ADD = 0)) then(select RESULT " +
                        "                                      from translate('IS_COMPLEX')) " +
                        "else (select RESULT " +
                        "from translate('IS_CALCULATION')) " +
                        "end) = '#_IS_COMPLEX' ) " +
                        "and((not exists(select first 1 1 " +
                        "from DIC_GOODS_LAB_NORMS N " +
                        "where N.GOODS_ID = dg.ID) " +
                        "and exists(select first 1 1 " +
                        "from DIC_CALCULATIONS C " +
                        "where C.HD_ID = DG.ID)) or(exists(select first 1 1 " +
                        "from DIC_GOODS_LAB_NORMS N " +
                        "where N.GOODS_ID = dg.ID) " +
                        "and exists(select first 1 1 " +
                        "from DIC_CALCULATIONS C " +
                        "where C.HD_ID = DG.ID) and dg.id = '65')) " +
                        "order by 9,3";


            // MessageBox.Show($"{query}");
            FbCommand cmd = new FbCommand(query, conn);

            try
            {
                //conn.Open();
                FbDataAdapter datareader = new FbDataAdapter(cmd);
                DataTable usuarios = new DataTable();

                datareader.Fill(usuarios);
                return usuarios;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                conn.Close();
            }
        }

        private static void LicensyaCheck()
        {
            string curFile = @"keyfile.dat";
            if (!File.Exists(curFile)) Environment.Exit(0); ;

            CryptoClass crypto = new CryptoClass();
            string date = crypto.GetDecodeKey(curFile).Substring(crypto.GetDecodeKey("keyfile.dat").IndexOf("|") + 1);
            if (DateTime.Parse(date).AddDays(1) <= DateTime.Now)
                try
                {
                    if (File.Exists(curFile))
                        File.Delete(curFile);
                }
                catch (IOException) { }
                finally
                { Environment.Exit(0); ; }

        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private static FbConnection GetConnection()
        {
            string connectionString =
                "User=SYSDBA;" +
                "Password=masterkey;" +
                @"Database=" + path_db + ";" +
                "Charset=UTF8;" +
                "Pooling=true;" +
                "ServerType=0;";

            FbConnection conn = new FbConnection(connectionString.ToString());

            conn.Open();

            return conn;
        }
    }
}
