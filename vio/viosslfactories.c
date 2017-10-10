/* Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "vio_priv.h"

#ifdef HAVE_OPENSSL

#define TLS_VERSION_OPTION_SIZE 256
#define SSL_CIPHER_LIST_SIZE 4096

#ifdef HAVE_YASSL
static const char tls_ciphers_list[]="DHE-RSA-AES256-SHA:DHE-RSA-AES128-SHA:"
  "AES128-RMD:DES-CBC3-RMD:DHE-RSA-AES256-RMD:"
  "DHE-RSA-AES128-RMD:DHE-RSA-DES-CBC3-RMD:"
  "AES256-SHA:RC4-SHA:RC4-MD5:DES-CBC3-SHA:"
  "DES-CBC-SHA:EDH-RSA-DES-CBC3-SHA:"
  "EDH-RSA-DES-CBC-SHA:AES128-SHA:AES256-RMD";
static const char tls_cipher_blocked[]= "!aNULL:!eNULL:!EXPORT:!LOW:!MD5:!DES:!RC2:!RC4:!PSK:";
#else
static const char tls_ciphers_list[]="ECDHE-ECDSA-AES128-GCM-SHA256:"
  "ECDHE-ECDSA-AES256-GCM-SHA384:"
  "ECDHE-RSA-AES128-GCM-SHA256:"
  "ECDHE-RSA-AES256-GCM-SHA384:"
  "ECDHE-ECDSA-AES128-SHA256:"
  "ECDHE-RSA-AES128-SHA256:"
  "ECDHE-ECDSA-AES256-SHA384:"
  "ECDHE-RSA-AES256-SHA384:"
  "DHE-RSA-AES128-GCM-SHA256:"
  "DHE-DSS-AES128-GCM-SHA256:"
  "DHE-RSA-AES128-SHA256:"
  "DHE-DSS-AES128-SHA256:"
  "DHE-DSS-AES256-GCM-SHA384:"
  "DHE-RSA-AES256-SHA256:"
  "DHE-DSS-AES256-SHA256:"
  "ECDHE-RSA-AES128-SHA:ECDHE-ECDSA-AES128-SHA:"
  "ECDHE-RSA-AES256-SHA:ECDHE-ECDSA-AES256-SHA:"
  "DHE-DSS-AES128-SHA:DHE-RSA-AES128-SHA:"
  "TLS_DHE_DSS_WITH_AES_256_CBC_SHA:DHE-RSA-AES256-SHA:"
  "AES128-GCM-SHA256:DH-DSS-AES128-GCM-SHA256:"
  "ECDH-ECDSA-AES128-GCM-SHA256:AES256-GCM-SHA384:"
  "DH-DSS-AES256-GCM-SHA384:ECDH-ECDSA-AES256-GCM-SHA384:"
  "AES128-SHA256:DH-DSS-AES128-SHA256:ECDH-ECDSA-AES128-SHA256:AES256-SHA256:"
  "DH-DSS-AES256-SHA256:ECDH-ECDSA-AES256-SHA384:AES128-SHA:"
  "DH-DSS-AES128-SHA:ECDH-ECDSA-AES128-SHA:AES256-SHA:"
  "DH-DSS-AES256-SHA:ECDH-ECDSA-AES256-SHA:DHE-RSA-AES256-GCM-SHA384:"
  "DH-RSA-AES128-GCM-SHA256:ECDH-RSA-AES128-GCM-SHA256:DH-RSA-AES256-GCM-SHA384:"
  "ECDH-RSA-AES256-GCM-SHA384:DH-RSA-AES128-SHA256:"
  "ECDH-RSA-AES128-SHA256:DH-RSA-AES256-SHA256:"
  "ECDH-RSA-AES256-SHA384:ECDHE-RSA-AES128-SHA:"
  "ECDHE-ECDSA-AES128-SHA:ECDHE-RSA-AES256-SHA:"
  "ECDHE-ECDSA-AES256-SHA:DHE-DSS-AES128-SHA:DHE-RSA-AES128-SHA:"
  "TLS_DHE_DSS_WITH_AES_256_CBC_SHA:DHE-RSA-AES256-SHA:"
  "AES128-SHA:DH-DSS-AES128-SHA:ECDH-ECDSA-AES128-SHA:AES256-SHA:"
  "DH-DSS-AES256-SHA:ECDH-ECDSA-AES256-SHA:DH-RSA-AES128-SHA:"
  "ECDH-RSA-AES128-SHA:DH-RSA-AES256-SHA:ECDH-RSA-AES256-SHA:DES-CBC3-SHA";
static const char tls_cipher_blocked[]= "!aNULL:!eNULL:!EXPORT:!LOW:!MD5:!DES:!RC2:!RC4:!PSK:"
  "!DHE-DSS-DES-CBC3-SHA:!DHE-RSA-DES-CBC3-SHA:"
  "!ECDH-RSA-DES-CBC3-SHA:!ECDH-ECDSA-DES-CBC3-SHA:"
  "!ECDHE-RSA-DES-CBC3-SHA:!ECDHE-ECDSA-DES-CBC3-SHA:";
#endif




static my_bool     ssl_algorithms_added    = FALSE;
static my_bool     ssl_error_strings_loaded= FALSE;

/*
  Diffie-Hellman key.
  Generated using: >openssl dhparam -5 -C 2048

  -----BEGIN DH PARAMETERS-----
  MIIBCAKCAQEAil36wGZ2TmH6ysA3V1xtP4MKofXx5n88xq/aiybmGnReZMviCPEJ
  46+7VCktl/RZ5iaDH1XNG1dVQmznt9pu2G3usU+k1/VB4bQL4ZgW4u0Wzxh9PyXD
  glm99I9Xyj4Z5PVE4MyAsxCRGA1kWQpD9/zKAegUBPLNqSo886Uqg9hmn8ksyU9E
  BV5eAEciCuawh6V0O+Sj/C3cSfLhgA0GcXp3OqlmcDu6jS5gWjn3LdP1U0duVxMB
  h/neTSCSvtce4CAMYMjKNVh9P1nu+2d9ZH2Od2xhRIqMTfAS1KTqF3VmSWzPFCjG
  mjxx/bg6bOOjpgZapvB6ABWlWmRmAAWFtwIBBQ==
  -----END DH PARAMETERS-----
 */
static unsigned char dh2048_p[]=
{
  0x8A, 0x5D, 0xFA, 0xC0, 0x66, 0x76, 0x4E, 0x61, 0xFA, 0xCA, 0xC0, 0x37,
  0x57, 0x5C, 0x6D, 0x3F, 0x83, 0x0A, 0xA1, 0xF5, 0xF1, 0xE6, 0x7F, 0x3C,
  0xC6, 0xAF, 0xDA, 0x8B, 0x26, 0xE6, 0x1A, 0x74, 0x5E, 0x64, 0xCB, 0xE2,
  0x08, 0xF1, 0x09, 0xE3, 0xAF, 0xBB, 0x54, 0x29, 0x2D, 0x97, 0xF4, 0x59,
  0xE6, 0x26, 0x83, 0x1F, 0x55, 0xCD, 0x1B, 0x57, 0x55, 0x42, 0x6C, 0xE7,
  0xB7, 0xDA, 0x6E, 0xD8, 0x6D, 0xEE, 0xB1, 0x4F, 0xA4, 0xD7, 0xF5, 0x41,
  0xE1, 0xB4, 0x0B, 0xE1, 0x98, 0x16, 0xE2, 0xED, 0x16, 0xCF, 0x18, 0x7D,
  0x3F, 0x25, 0xC3, 0x82, 0x59, 0xBD, 0xF4, 0x8F, 0x57, 0xCA, 0x3E, 0x19,
  0xE4, 0xF5, 0x44, 0xE0, 0xCC, 0x80, 0xB3, 0x10, 0x91, 0x18, 0x0D, 0x64,
  0x59, 0x0A, 0x43, 0xF7, 0xFC, 0xCA, 0x01, 0xE8, 0x14, 0x04, 0xF2, 0xCD,
  0xA9, 0x2A, 0x3C, 0xF3, 0xA5, 0x2A, 0x83, 0xD8, 0x66, 0x9F, 0xC9, 0x2C,
  0xC9, 0x4F, 0x44, 0x05, 0x5E, 0x5E, 0x00, 0x47, 0x22, 0x0A, 0xE6, 0xB0,
  0x87, 0xA5, 0x74, 0x3B, 0xE4, 0xA3, 0xFC, 0x2D, 0xDC, 0x49, 0xF2, 0xE1,
  0x80, 0x0D, 0x06, 0x71, 0x7A, 0x77, 0x3A, 0xA9, 0x66, 0x70, 0x3B, 0xBA,
  0x8D, 0x2E, 0x60, 0x5A, 0x39, 0xF7, 0x2D, 0xD3, 0xF5, 0x53, 0x47, 0x6E,
  0x57, 0x13, 0x01, 0x87, 0xF9, 0xDE, 0x4D, 0x20, 0x92, 0xBE, 0xD7, 0x1E,
  0xE0, 0x20, 0x0C, 0x60, 0xC8, 0xCA, 0x35, 0x58, 0x7D, 0x3F, 0x59, 0xEE,
  0xFB, 0x67, 0x7D, 0x64, 0x7D, 0x8E, 0x77, 0x6C, 0x61, 0x44, 0x8A, 0x8C,
  0x4D, 0xF0, 0x12, 0xD4, 0xA4, 0xEA, 0x17, 0x75, 0x66, 0x49, 0x6C, 0xCF,
  0x14, 0x28, 0xC6, 0x9A, 0x3C, 0x71, 0xFD, 0xB8, 0x3A, 0x6C, 0xE3, 0xA3,
  0xA6, 0x06, 0x5A, 0xA6, 0xF0, 0x7A, 0x00, 0x15, 0xA5, 0x5A, 0x64, 0x66,
  0x00, 0x05, 0x85, 0xB7,
};

static unsigned char dh2048_g[]={
  0x05,
};

static DH *get_dh2048(void)
{
  DH *dh;
  if ((dh=DH_new()))
  {
    dh->p=BN_bin2bn(dh2048_p,sizeof(dh2048_p),NULL);
    dh->g=BN_bin2bn(dh2048_g,sizeof(dh2048_g),NULL);
    if (! dh->p || ! dh->g)
    {
      DH_free(dh);
      dh=0;
    }
  }
  return(dh);
}


static void
report_errors()
{
  unsigned long	l;
  const char*	file;
  const char*	data;
  int		line,flags;

  DBUG_ENTER("report_errors");

  while ((l=ERR_get_error_line_data(&file,&line,&data,&flags)) != 0)
  {
#ifndef DBUG_OFF				/* Avoid warning */
    char buf[200];
    DBUG_PRINT("error", ("OpenSSL: %s:%s:%d:%s\n", ERR_error_string(l,buf),
			 file,line,(flags & ERR_TXT_STRING) ? data : "")) ;
#endif
  }
  DBUG_VOID_RETURN;
}

static const char*
ssl_error_string[] = 
{
  "No error",
  "Unable to get certificate",
  "Unable to get private key",
  "Private key does not match the certificate public key",
  "SSL_CTX_set_default_verify_paths failed",
  "Failed to set ciphers to use",
  "SSL_CTX_new failed",
  "SSL_CTX_set_tmp_dh failed",
  "TLS version is invalid"
};

const char*
sslGetErrString(enum enum_ssl_init_error e)
{
  DBUG_ASSERT(SSL_INITERR_NOERROR < e && e < SSL_INITERR_LASTERR);
  return ssl_error_string[e];
}

static int
vio_set_cert_stuff(SSL_CTX *ctx, const char *cert_file, const char *key_file,
                   enum enum_ssl_init_error* error)
{
  DBUG_ENTER("vio_set_cert_stuff");
  DBUG_PRINT("enter", ("ctx: 0x%lx  cert_file: %s  key_file: %s",
		       (long) ctx, cert_file, key_file));

  if (!cert_file &&  key_file)
    cert_file= key_file;
  
  if (!key_file &&  cert_file)
    key_file= cert_file;

  if (cert_file &&
      SSL_CTX_use_certificate_file(ctx, cert_file, SSL_FILETYPE_PEM) <= 0)
  {
    *error= SSL_INITERR_CERT;
    DBUG_PRINT("error",("%s from file '%s'", sslGetErrString(*error), cert_file));
    DBUG_EXECUTE("error", ERR_print_errors_fp(DBUG_FILE););
    fprintf(stderr, "SSL error: %s from '%s'\n", sslGetErrString(*error),
            cert_file);
    fflush(stderr);
    DBUG_RETURN(1);
  }

  if (key_file &&
      SSL_CTX_use_PrivateKey_file(ctx, key_file, SSL_FILETYPE_PEM) <= 0)
  {
    *error= SSL_INITERR_KEY;
    DBUG_PRINT("error", ("%s from file '%s'", sslGetErrString(*error), key_file));
    DBUG_EXECUTE("error", ERR_print_errors_fp(DBUG_FILE););
    fprintf(stderr, "SSL error: %s from '%s'\n", sslGetErrString(*error),
            key_file);
    fflush(stderr);
    DBUG_RETURN(1);
  }

  /*
    If we are using DSA, we can copy the parameters from the private key
    Now we know that a key and cert have been set against the SSL context
  */
  if (cert_file && !SSL_CTX_check_private_key(ctx))
  {
    *error= SSL_INITERR_NOMATCH;
    DBUG_PRINT("error", ("%s",sslGetErrString(*error)));
    DBUG_EXECUTE("error", ERR_print_errors_fp(DBUG_FILE););
    fprintf(stderr, "SSL error: %s\n", sslGetErrString(*error));
    fflush(stderr);
    DBUG_RETURN(1);
  }

  DBUG_RETURN(0);
}


void ssl_start()
{
  if (!ssl_algorithms_added)
  {
    ssl_algorithms_added= TRUE;
    SSL_library_init();
    OpenSSL_add_all_algorithms();

  }

  if (!ssl_error_strings_loaded)
  {
    ssl_error_strings_loaded= TRUE;
    SSL_load_error_strings();
  }
}

long process_tls_version(const char *tls_version)
{
  const char *separator= ",";
  char *token, *lasts= NULL;

  const char *tls_version_name_list[]= {"TLSv1"
#ifdef SSL_OP_NO_TLSv1_1
                                        ,"TLSv1.1"
#endif
#ifdef SSL_OP_NO_TLSv1_2
                                        ,"TLSv1.2"
#endif
                                       };

  const char ctx_flag_default[]=
#ifdef SSL_OP_NO_TLSv1_2
    "TLSv1,TLSv1.1,TLSv1.2";
#elif defined(SSL_OP_NO_TLSv1_1)
    "TLSv1,TLSv1.1";
#else
    "TLSv1";
#endif

  const long tls_ctx_list[]= {
                              SSL_OP_NO_TLSv1
#ifdef SSL_OP_NO_TLSv1_1
                              ,SSL_OP_NO_TLSv1_1
#endif
#ifdef SSL_OP_NO_TLSv1_2
                              ,SSL_OP_NO_TLSv1_2
#endif
                              };


  size_t tls_versions_count= sizeof(tls_ctx_list) / sizeof(tls_ctx_list[0]);
  long tls_ctx_flag= 0;
  unsigned int index= 0;
  char tls_version_option[TLS_VERSION_OPTION_SIZE]= "";
  int tls_found= 0;

  for (index= 0; index < tls_versions_count; index++)
    tls_ctx_flag|= tls_ctx_list[index];

  if (!tls_version || !my_strcasecmp(&my_charset_latin1, tls_version,
                                        ctx_flag_default))
    return 0;


  if (strlen(tls_version) - 1 > sizeof(tls_version_option))
    return -1;

  strncpy(tls_version_option, tls_version, sizeof(tls_version_option));
  token= my_strtok_r(tls_version_option, separator, &lasts);
  while (token)
  {
    for (index= 0; index < tls_versions_count; index++)
    {
      if (!my_strcasecmp(&my_charset_latin1, tls_version_name_list[index],
            token))
      {
        tls_found= 1;
        tls_ctx_flag= tls_ctx_flag & (~tls_ctx_list[index]);
        break;
      }
    }
    token= my_strtok_r(NULL, separator, &lasts);
  }

  if (!tls_found)
    return -1;
  else
    return tls_ctx_flag;
}




/************************ VioSSLFd **********************************/
static struct st_VioSSLFd *
new_VioSSLFd(const char *key_file, const char *cert_file,
             const char *ca_file, const char *ca_path,
             const char *cipher, my_bool is_client,
             enum enum_ssl_init_error *error,
             const char *crl_file, const char *crl_path,
             const long ssl_ctx_flags)
{
  DH *dh;
  struct st_VioSSLFd *ssl_fd;
  long ssl_ctx_options= SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3;
  int ret_set_cipherlist= 0;
  char cipher_list[SSL_CIPHER_LIST_SIZE]= {0};
  DBUG_ENTER("new_VioSSLFd");
  DBUG_PRINT("enter",
             ("key_file: '%s'  cert_file: '%s'  ca_file: '%s'  ca_path: '%s'  "
              "cipher: '%s' crl_file: '%s' crl_path: '%s' ",
              key_file ? key_file : "NULL",
              cert_file ? cert_file : "NULL",
              ca_file ? ca_file : "NULL",
              ca_path ? ca_path : "NULL",
              cipher ? cipher : "NULL",
              crl_file ? crl_file : "NULL",
              crl_path ? crl_path : "NULL"));

  if (ssl_ctx_flags < 0)
  {
    *error= SSL_TLS_VERSION_INVALID;
    DBUG_PRINT("error", ("TLS version invalid : %s", sslGetErrString(*error)));
    report_errors();
    DBUG_RETURN(0);
  }

  ssl_ctx_options= (ssl_ctx_options | ssl_ctx_flags) &
    (SSL_OP_NO_SSLv2 |
     SSL_OP_NO_SSLv3 |
     SSL_OP_NO_TLSv1
#ifdef SSL_OP_NO_TLSv1_1
     | SSL_OP_NO_TLSv1_1
#endif
#ifdef SSL_OP_NO_TLSv1_2
     | SSL_OP_NO_TLSv1_2
#endif
     );


  ssl_start();

  if (!(ssl_fd= ((struct st_VioSSLFd*)
                 my_malloc(sizeof(struct st_VioSSLFd),MYF(0)))))
    DBUG_RETURN(0);

  if (!(ssl_fd->ssl_context= SSL_CTX_new(is_client ?
                                         SSLv23_client_method() :
                                         SSLv23_server_method())))
  {
    *error= SSL_INITERR_MEMFAIL;
    DBUG_PRINT("error", ("%s", sslGetErrString(*error)));
    report_errors();
    my_free(ssl_fd);
    DBUG_RETURN(0);
  }

  SSL_CTX_set_options(ssl_fd->ssl_context, ssl_ctx_options);

  /*
    Set the ciphers that can be used
    NOTE: SSL_CTX_set_cipher_list will return 0 if
    none of the provided ciphers could be selected
  */

  DBUG_ASSERT(strlen(tls_cipher_blocked) + 1 <= sizeof(cipher_list));
  strcat(cipher_list, tls_cipher_blocked);
  if (cipher)
  {
    if (strlen(cipher_list) + strlen(cipher) + 1 > sizeof(cipher_list))
    {
      *error= SSL_INITERR_CIPHERS;
      DBUG_PRINT("error", ("User specified cipher too long"));
      SSL_CTX_free(ssl_fd->ssl_context);
      my_free(ssl_fd);
      DBUG_RETURN(0);
    }
    strcat(cipher_list, cipher);
  }
  else
  {
    DBUG_ASSERT(strlen(cipher_list) + strlen(tls_ciphers_list) + 1
                <= sizeof(cipher_list));
    strcat(cipher_list, tls_ciphers_list);
  }

  if (ret_set_cipherlist == SSL_CTX_set_cipher_list(ssl_fd->ssl_context,
                                                    cipher_list))
  {
    *error= SSL_INITERR_CIPHERS;
    DBUG_PRINT("error", ("%s", sslGetErrString(*error)));
    report_errors();
    SSL_CTX_free(ssl_fd->ssl_context);
    my_free(ssl_fd);
    DBUG_RETURN(0);
  }

  /* Load certs from the trusted ca */
  if (SSL_CTX_load_verify_locations(ssl_fd->ssl_context, ca_file, ca_path) <= 0)
  {
    DBUG_PRINT("warning", ("SSL_CTX_load_verify_locations failed"));
    if (ca_file || ca_path)
    {
      /* fail only if ca file or ca path were supplied and looking into 
         them fails. */
      *error= SSL_INITERR_BAD_PATHS;
      DBUG_PRINT("error", ("SSL_CTX_load_verify_locations failed : %s", 
                 sslGetErrString(*error)));
      report_errors();
      SSL_CTX_free(ssl_fd->ssl_context);
      my_free(ssl_fd);
      DBUG_RETURN(0);
    }

    /* otherwise go use the defaults */
    if (SSL_CTX_set_default_verify_paths(ssl_fd->ssl_context) == 0)
    {
      *error= SSL_INITERR_BAD_PATHS;
      DBUG_PRINT("error", ("%s", sslGetErrString(*error)));
      report_errors();
      SSL_CTX_free(ssl_fd->ssl_context);
      my_free(ssl_fd);
      DBUG_RETURN(0);
    }
  }

  if (crl_file || crl_path)
  {
#ifdef HAVE_YASSL
    DBUG_PRINT("warning", ("yaSSL doesn't support CRL"));
    DBUG_ASSERT(0);
#else
    X509_STORE *store= SSL_CTX_get_cert_store(ssl_fd->ssl_context);
    /* Load crls from the trusted ca */
    if (X509_STORE_load_locations(store, crl_file, crl_path) == 0 ||
        X509_STORE_set_flags(store,
                             X509_V_FLAG_CRL_CHECK | 
                             X509_V_FLAG_CRL_CHECK_ALL) == 0)
    {
      DBUG_PRINT("warning", ("X509_STORE_load_locations for CRL failed"));
      *error= SSL_INITERR_BAD_PATHS;
      DBUG_PRINT("error", ("%s", sslGetErrString(*error)));
      report_errors();
      SSL_CTX_free(ssl_fd->ssl_context);
      my_free(ssl_fd);
      DBUG_RETURN(0);
    }
#endif
  }

  if (vio_set_cert_stuff(ssl_fd->ssl_context, cert_file, key_file, error))
  {
    DBUG_PRINT("error", ("vio_set_cert_stuff failed"));
    report_errors();
    SSL_CTX_free(ssl_fd->ssl_context);
    my_free(ssl_fd);
    DBUG_RETURN(0);
  }

  /* DH stuff */
  dh= get_dh2048();
  if (SSL_CTX_set_tmp_dh(ssl_fd->ssl_context, dh) == 0)
  {
    *error= SSL_INITERR_DHFAIL;
    DBUG_PRINT("error", ("%s", sslGetErrString(*error)));
    report_errors();
    DH_free(dh);
    SSL_CTX_free(ssl_fd->ssl_context);
    my_free(ssl_fd);
    DBUG_RETURN(0);
  }
  DH_free(dh);

  DBUG_PRINT("exit", ("OK 1"));

  DBUG_RETURN(ssl_fd);
}


/************************ VioSSLConnectorFd **********************************/
struct st_VioSSLFd *
new_VioSSLConnectorFd(const char *key_file, const char *cert_file,
                      const char *ca_file, const char *ca_path,
                      const char *cipher, enum enum_ssl_init_error* error,
                      const char *crl_file, const char *crl_path,
                      const long ssl_ctx_flags)
{
  struct st_VioSSLFd *ssl_fd;
  int verify= SSL_VERIFY_PEER;

  /*
    Turn off verification of servers certificate if both
    ca_file and ca_path is set to NULL
  */
  if (ca_file == 0 && ca_path == 0)
    verify= SSL_VERIFY_NONE;

  if (!(ssl_fd= new_VioSSLFd(key_file, cert_file, ca_file,
                             ca_path, cipher, TRUE, error,
                             crl_file, crl_path,
                             ssl_ctx_flags)))
  {
    return 0;
  }

  /* Init the VioSSLFd as a "connector" ie. the client side */

  SSL_CTX_set_verify(ssl_fd->ssl_context, verify, NULL);

  return ssl_fd;
}


/************************ VioSSLAcceptorFd **********************************/
struct st_VioSSLFd *
new_VioSSLAcceptorFd(const char *key_file, const char *cert_file,
		     const char *ca_file, const char *ca_path,
		     const char *cipher, enum enum_ssl_init_error* error,
                     const char *crl_file, const char *crl_path,
                     const long ssl_ctx_flags)
{
  struct st_VioSSLFd *ssl_fd;
  int verify= SSL_VERIFY_PEER | SSL_VERIFY_CLIENT_ONCE;
  if (!(ssl_fd= new_VioSSLFd(key_file, cert_file, ca_file,
                             ca_path, cipher, FALSE, error,
                             crl_file, crl_path,
                             ssl_ctx_flags)))
  {
    return 0;
  }
  /* Init the the VioSSLFd as a "acceptor" ie. the server side */

  /* Set max number of cached sessions, returns the previous size */
  SSL_CTX_sess_set_cache_size(ssl_fd->ssl_context, 128);

  SSL_CTX_set_verify(ssl_fd->ssl_context, verify, NULL);

  /*
    Set session_id - an identifier for this server session
    Use the ssl_fd pointer
   */
  SSL_CTX_set_session_id_context(ssl_fd->ssl_context,
				 (const unsigned char *)ssl_fd,
				 sizeof(ssl_fd));

  return ssl_fd;
}

void free_vio_ssl_acceptor_fd(struct st_VioSSLFd *fd)
{
  SSL_CTX_free(fd->ssl_context);
  my_free(fd);
}
#endif /* HAVE_OPENSSL */
