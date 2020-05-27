using System;
using System.IO;
using System.Management;
using System.Security.Cryptography;
using System.Windows.Forms;

namespace ExportPriceForWeb
{
    /*Класс защиты приложения, 
    * запускается при загрузке формы,
    * при отсутствии ключа либо несовпадении данных в нем
    * приложение закрывается*/
    class CryptoClass
    {
        RijndaelManaged Rijndael;

        public CryptoClass()
        {
            Rijndael = new RijndaelManaged();
        }
        //Метод для получения MotherBoardID 
        string GetMotherBoardID()
        {
            string MotherBoardID = string.Empty;
            SelectQuery query = new SelectQuery("Win32_BaseBoard");
            ManagementObjectSearcher searcher = new ManagementObjectSearcher(query);
            ManagementObjectCollection.ManagementObjectEnumerator enumerator = searcher.Get().GetEnumerator();
            while (enumerator.MoveNext())
            {
                ManagementObject info = (ManagementObject)enumerator.Current;
                MotherBoardID = info["SerialNumber"].ToString().Trim();
            }
            return MotherBoardID;
        }

        string GetVolumeSerial(string strDriveLetter = "C")
        {
            ManagementObject VolumeSerial = new ManagementObject(string.Format("win32_logicaldisk.deviceid=\"{0}:\"", strDriveLetter));
            VolumeSerial.Get();
            return VolumeSerial["VolumeSerialNumber"].ToString().Trim();
        }

        //Метод запускаемый при загрузке приложения
        public bool Form_LoadTrue()
        {

            string date = DateTime.Now.ToShortDateString().ToString();
            //Данные с целевого компьютера
            //string number = GetMotherBoardID() + /*GetProcessorID() +*/ GetVolumeSerial() +"|"+ date;
            string number = GetMotherBoardID() + /*GetProcessorID() +*/ GetVolumeSerial(); //убираем дату

            //Файл ключа присутствует
            if (File.Exists("keyfile.dat"))
            {
                if (!DecodeKey(number, "keyfile.dat"))
                {
                    MessageBox.Show("Файл ключа не верный!" + "\n" +
                                    "Данные скопированы в буфер обмена." + "\n" +
                                    "Сообщите их разработчику для получения файла ключа!",
                                    "Регистрация");
                    Clipboard.SetText(number);
                    return false;
                }
            }
            else
            {
                //Файл ключа отсутствует
                MessageBox.Show("Файл ключа отсутствует!" + "\n" +
                                "Данные скопированы в буфер обмена" + "\n" +
                                "Сообщите их разработчику для получения файла ключа!",
                                "Регистрация");
                try
                {
                    Clipboard.SetText(number);
                }
                catch (Exception)
                {
                    MessageBox.Show("Ошибка записи в буфер",
                    "Error", MessageBoxButtons.OK, MessageBoxIcon.Warning // for Warning  
                                                                          //MessageBoxIcon.Error // for Error 
                                                                          //MessageBoxIcon.Information  // for Information
                                                                          //MessageBoxIcon.Question // for Question
                           );
                }

                return false;
            }

            return true;
        }

        //Метод декриптования файла ключа
        //Возвращает true или false в зависимости от того,
        //совпал ли ключ в файле и входная строка
        public bool DecodeKey(string inString, string path)
        {
            string decryptstring = null;
            byte[] key = new byte[0x20];
            for (int i = 0; i <= 0x1f; i++)
                key[i] = 0x1f;
            Rijndael.Key = key;
            using (FileStream fs = File.Open(path, FileMode.Open))
            {
                //FileStream fs = new FileStream(path, FileMode.Open);
                byte[] IV = new byte[Rijndael.IV.Length];
                fs.Read(IV, 0, IV.Length);
                Rijndael.IV = IV;
                ICryptoTransform transformer = Rijndael.CreateDecryptor();
                CryptoStream cs = new CryptoStream(fs, transformer, CryptoStreamMode.Read);
                StreamReader sr = new StreamReader(cs);
                decryptstring = sr.ReadToEnd();

                CryptoClass crypto = new CryptoClass();

                decryptstring = decryptstring.Substring(0, decryptstring.IndexOf("|"));

                if (!(decryptstring == inString))
                    return false;
                else
                    return true;
            }
        }

        public string GetDecodeKey(string path)
        {
            string decryptstring = null;
            byte[] key = new byte[0x20];
            for (int i = 0; i <= 0x1f; i++)
                key[i] = 0x1f;
            Rijndael.Key = key;
            using (FileStream fs = File.Open(path, FileMode.Open))
            {
                byte[] IV = new byte[Rijndael.IV.Length];
                fs.Read(IV, 0, IV.Length);
                Rijndael.IV = IV;
                ICryptoTransform transformer = Rijndael.CreateDecryptor();
                CryptoStream cs = new CryptoStream(fs, transformer, CryptoStreamMode.Read);
                StreamReader sr = new StreamReader(cs);
                decryptstring = sr.ReadToEnd();
                return decryptstring;
            }
        }
    }
}
