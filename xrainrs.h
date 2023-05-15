#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

/// A header of XRAIN, which explains the number of blocks, data length(size), bottom left, upper right, etc...
///
/// XRAINファイルのヘッダー
/// 詳しくはドキュメントを参照されたい。
/// かなり未実装。
/// TODO:まじでいつか書く
struct XrainHeader {
  ///地整識別
  uint8_t owner;
  ///データ種別3
  /// 1byte:対象エリアの地整識別コード
  uint16_t mesh_kind;
  ///観測日時(WIP)
  unsigned long long datetime;
  ///応答ステータス
  uint8_t response_status;
  ///ブロック数
  uint16_t block_num;
  ///ファイルのサイズ
  uint32_t data_size;
  ///南西端の1次メッシュコード
  uint16_t bottom_left;
  ///北東端の1次メッシュコード
  uint16_t top_right;
};

/// Has quality and rainfall data.(cf. XRAIN document)
///
/// 雨量データと品質データ
///
struct XrainCell {
  ///品質データ
  uint16_t quality;
  ///雨量
  uint16_t strength;
};

struct CXrainDataset {
  ///TODO:CXrainDatasetをmem::forgetした後、
  /// XRAINheaderはどうなるんだろう。Dropされるのかな？
  XrainHeader header;
  ///配列のポインタ。
  XrainCell *ptr;
  ///The number of XrainCell.
  uint64_t length;
};

extern "C" {

/// XRAINデータを
CXrainDataset open(const char *file_path);

} // extern "C"
