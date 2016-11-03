﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MysqlBatchAdd
{
    public class PersonAddHelper : BatchAddBase<Person>
    {
        protected override string InsertHead
        {
            get
            {
                return @"insert into sys_systemname(
code,name) values ";
            }
        }

        public override void BatchAdd(Person m)
        {
            this.InsertBodyList.Add(" ('"+m.code+"','"+m.name+"')");
        }
    }
}
