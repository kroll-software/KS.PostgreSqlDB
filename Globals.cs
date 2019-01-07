/*
{*******************************************************************}
{                                                                   }
{          KS-GANTT Library                                         }
{          A Project Planning Solution for Professionals            }
{                                                                   }
{          Copyright (c) 2009 - 2015 by Kroll-Software,             }
{          Altdorf, Switzerland, All Rights Reserved                }
{          www.kroll-software.ch                                    }
{                                                                   }
{   Dual licensed under                                             }
{   (1) GNU Public License version 2 (GPLv2)                        }
{   http://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html        }
{                                                                   }
{   and if this doesn't fit for you, there are                      }
{   (2) Commercial licenses available.                              }
{                                                                   }
{   This file belongs to the                                        }
{   KS-Gantt WinForms Control library v. 5.0.2                      }
{                                                                   }
{*******************************************************************}
*/

using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace KS.PostgreSqlDB
{
    public static class Globals
    {
        public static string RegistrySectionKey = "Software\\Kroll-Software\\PostgreSqlDB\\Settings";

        public delegate string GetSettingDelegate(string key);
        public static GetSettingDelegate GetSetting;

        public delegate void SaveSettingDelegate(string key, string value);
        public static SaveSettingDelegate SaveSetting;        
    }
}