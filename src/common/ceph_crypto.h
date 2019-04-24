#ifndef CEPH_CRYPTO_H
#define CEPH_CRYPTO_H

#include "acconfig.h"

#define CEPH_CRYPTO_MD5_DIGESTSIZE 16
#define CEPH_CRYPTO_HMACSHA1_DIGESTSIZE 20
#define CEPH_CRYPTO_SHA1_DIGESTSIZE 20
#define CEPH_CRYPTO_HMACSHA256_DIGESTSIZE 32
#define CEPH_CRYPTO_SHA256_DIGESTSIZE 32

#ifdef USE_CRYPTOPP
# define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1
#include <string.h>
#include <cryptopp/md5.h>
#include <cryptopp/sha.h>
#include <cryptopp/hmac.h>
#include <stdexcept>

// reinclude our assert to clobber the system one
# include "include/assert.h"

namespace ceph {
  namespace crypto {

    class DigestException : public std::runtime_error
	{
	public:
		DigestException(const char* what_arg) : runtime_error(what_arg)
			{}
	};
  
    void assert_init();
    void init(CephContext *cct);
    // @param shared true if the the underlying crypto library could be shared
    //               with the application linked against the Ceph library.
    // @note we do extra global cleanup specific to the underlying crypto
    //       library, if @c shared is @c false.
    void shutdown(bool shared=true);

    using CryptoPP::Weak::MD5;
    using CryptoPP::SHA1;
    using CryptoPP::SHA256;

    class HMACSHA1: public CryptoPP::HMAC<CryptoPP::SHA1> {
    public:
	 HMACSHA1 (const byte *key, size_t length)
	: CryptoPP::HMAC<CryptoPP::SHA1>(key, length)
	{
	}
      ~HMACSHA1();
    };

    class HMACSHA256: public CryptoPP::HMAC<CryptoPP::SHA256> {
    public:
      HMACSHA256 (const byte *key, size_t length)
        : CryptoPP::HMAC<CryptoPP::SHA256>(key, length)
        {
        }
      ~HMACSHA256();
    };
  }
}
#elif defined(USE_NSS)
// you *must* use CRYPTO_CXXFLAGS in CMakeLists.txt for including this include
# include <nss.h>
# include <pk11pub.h>
#include <stdexcept>

// NSS thinks a lot of fairly fundamental operations might potentially
// fail, because it has been written to support e.g. smartcards doing all
// the crypto operations. We don't want to contaminate too much code
// with error checking, and just say these really should never fail.
// This assert MUST NOT be compiled out, even on non-debug builds.
# include "include/assert.h"

// ugly bit of CryptoPP that we have to emulate here :(
typedef unsigned char byte;

namespace ceph {
  namespace crypto {
    // workaround for no PK11_ImportSymKey in FIPS mode
    PK11SymKey *PK11_ImportSymKey_FIPS(
	PK11SlotInfo *slot,
	CK_MECHANISM_TYPE type,
	PK11Origin origin,
	CK_ATTRIBUTE_TYPE operation,
	SECItem *key,
	void *wincx);
  } // namespace crypto
} // namespace

namespace ceph {
  namespace crypto {

    class DigestException : public std::runtime_error
	{
	public:
		using runtime_error = std::runtime_error;

		DigestException(const char* what_arg) : runtime_error(what_arg)
			{}
	};

	void assert_init();
    void init(CephContext *cct);
    void shutdown(bool shared=true);

	class Digest {
    private:
      PK11Context *ctx;
      size_t digest_size;
    public:
      Digest (SECOidTag _type, size_t _digest_size)
		  : digest_size(_digest_size) {
		  ctx = PK11_CreateDigestContext(_type);
		  if (! ctx) {
			  throw DigestException("PK11_CreateDigestContext() failed");
		  }
		  Restart();
      }

      ~Digest () {
		  PK11_DestroyContext(ctx, PR_TRUE);
      }

      void Restart() {
		  SECStatus s;
		  s = PK11_DigestBegin(ctx);
		  if (s != SECSuccess) {
			  throw DigestException("PK11_DigestBegin() failed");
		  }
      }

      void Update (const byte *input, size_t length) {
		  if (length) {
			  SECStatus s;
			  s = PK11_DigestOp(ctx, input, length);
			  if (s != SECSuccess) {
				  throw DigestException("PK11_DigestOp() failed");
			  }
		  }
      }

	  void Final (byte *digest) {
		  SECStatus s;
		  unsigned int dummy;
		  s = PK11_DigestFinal(ctx, digest, &dummy, digest_size);
		  if (! (s == SECSuccess) &&
			  (dummy == digest_size)) {
			  throw DigestException("PK11_DigestFinal() failed");
		  }
		  Restart();
	  }
    };

    class MD5 : public Digest {
    public:
      MD5 () : Digest(SEC_OID_MD5, CEPH_CRYPTO_MD5_DIGESTSIZE) { }
    };

    class SHA1 : public Digest {
    public:
      SHA1 () : Digest(SEC_OID_SHA1, CEPH_CRYPTO_SHA1_DIGESTSIZE) { }
    };

    class SHA256 : public Digest {
    public:
      SHA256 () : Digest(SEC_OID_SHA256, CEPH_CRYPTO_SHA256_DIGESTSIZE) { }
    };

    class HMAC {
    private:
      PK11SlotInfo *slot;
      PK11SymKey *symkey;
      PK11Context *ctx;
      unsigned int digest_size;
    public:
      HMAC (CK_MECHANISM_TYPE cktype, unsigned int digestsize,
			const byte *key, size_t length) {
        digest_size = digestsize;
	slot = PK11_GetBestSlot(cktype, NULL);
	if (! slot) {
		throw DigestException("PK11_GetBestSlot() failed");
	}

	SECItem keyItem;
	keyItem.type = siBuffer;
	keyItem.data = (unsigned char*)key;
	keyItem.len = length;
	symkey = PK11_ImportSymKey_FIPS(slot, cktype, PK11_OriginUnwrap,
					CKA_SIGN,  &keyItem, NULL);
	if (! symkey) {
		throw DigestException("PK11_ImportSymKey_FIPS() failed");
	}

	SECItem param;
	param.type = siBuffer;
	param.data = NULL;
	param.len = 0;
	ctx = PK11_CreateContextBySymKey(cktype, CKA_SIGN, symkey, &param);
	if (! ctx) {
		throw DigestException("PK11_CreateContextBySymKey() failed");
	}
	Restart();
   }

   ~HMAC ();

   void Restart() {
	   SECStatus s;
	   s = PK11_DigestBegin(ctx);
	   if (s != SECSuccess) {
		   throw DigestException("PK11_DigestBegin() failed");
	   }
   }

   void Update (const byte *input, size_t length) {
	   SECStatus s;
	   s = PK11_DigestOp(ctx, input, length);
	   if (s != SECSuccess) {
		   throw DigestException("PK11_DigestOp() failed");
	   }
   }

   void Final (byte *digest) {
	   SECStatus s;
	   unsigned int dummy;
	   s = PK11_DigestFinal(ctx, digest, &dummy, digest_size);
	   if (! (s == SECSuccess) &&
		   (dummy == digest_size)) {
		   throw DigestException("PK11_DigestFinal() failed");
	   }
	   Restart();
     }
    };

    class HMACSHA1 : public HMAC {
    public:
      HMACSHA1 (const byte *key, size_t length)
		  : HMAC(CKM_SHA_1_HMAC, CEPH_CRYPTO_HMACSHA1_DIGESTSIZE, key, length)
			{}
    };

    class HMACSHA256 : public HMAC {
    public:
      HMACSHA256 (const byte *key, size_t length)
		  : HMAC(CKM_SHA256_HMAC, CEPH_CRYPTO_HMACSHA256_DIGESTSIZE, key, length)
			{}
    };
  } /* namespace crypto */
} /* namespace ceph */

#else
// cppcheck-suppress preprocessorErrorDirective
# error "No supported crypto implementation found."
#endif

#endif
