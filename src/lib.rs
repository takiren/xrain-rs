use anyhow::{anyhow, Ok, Result};
use csv::Writer;
use ndarray::prelude::*;
use nom::{
    bytes, character,
    error::{Error, ErrorKind},
    Err, IResult, Needed, ToUsize,
};
use std::{io::Read, collections::BTreeMap};
use std::path::{Path, PathBuf};
use std::{
    any,
    ffi::{c_char, c_ulonglong, CStr},
};

/// A header of XRAIN, which explains the number of blocks, data length(size), bottom left, upper right, etc...
///
/// XRAINファイルのヘッダー
/// 詳しくはドキュメントを参照されたい。
/// かなり未実装。
/// TODO:まじでいつか書く

#[repr(C)]
#[derive(Debug)]
pub struct XrainHeader {
    ///地整識別
    owner: u8,
    ///データ種別3
    /// 1byte:対象エリアの地整識別コード
    mesh_kind: u16,
    ///観測日時(WIP)
    datetime: c_ulonglong,
    ///応答ステータス
    response_status: u8,
    ///ブロック数
    block_num: u16,
    ///ファイルのサイズ
    data_size: u32,
    ///南西端の1次メッシュコード
    bottom_left_lat: u8,
    bottom_left_lon: u8,
    ///北東端の1次メッシュコード
    top_right_lat: u8,
    top_right_lon: u8,
}

///これいる？
impl Default for XrainHeader {
    fn default() -> Self {
        Self {
            owner: 71,
            mesh_kind: 0,
            datetime: 0,
            response_status: 0,
            block_num: 0,
            data_size: 0,
            bottom_left_lat: 0,
            bottom_left_lon: 0,
            top_right_lat: 0,
            top_right_lon: 0,
        }
    }
}

/// A block header stores structure of block which consists of multiple cells.
/// One block consists of multiple cells which contains rainfall-data(1600 grided data contained).
///
/// XRAINファイル内のブロックヘッダー
/// ブロック：連続するセルの集合
///
#[repr(C)]
#[derive(Debug)]
pub struct XrainBlockHeader {
    /// 先頭ブロックの1次メッシュコード上2桁
    lat: u8,
    /// 先頭ブロックの1次メッシュコード下2桁
    lon: u8,

    /// 先頭ブロックの2次メッシュコード下1桁
    first_x: u8,
    /// 先頭ブロックの2次メッシュコード上1桁
    first_y: u8,
    /// 連続するセルの個数。
    length: u8,
}

impl XrainBlockHeader {
    ///長さを取得
    fn len(&self) -> u8 {
        self.length
    }
}

/// Secondary mesh which contains rainfall.
/// 2次メッシュ単位のデータ
///
#[derive(Debug)]
pub struct SecondaryMesh {
    /// 1次メッシュの下２桁
    primary_lon_code: u8,
    /// 1次メッシュの上２桁
    primary_lat_code: u8,
    /// 緯度、経度に8分割された2次メッシュの番号
    /// x:経度方向 増加方向　西から東
    /// y:緯度方向 増加方向　南から北
    secondary_lon_code: u8,
    secondary_lat_code: u8,
    /// 雨量データのvec40*40
    /// assert_eq!(xrain_cells.len(),1600);
    xrain_cells: CellComposite,
}

/// Rainfall in one-fourth third mesh in secondary mesh.
/// 2次メッシュ内の1/4倍3次メッシュでの雨量データ
///
type CellComposite = Vec<XrainCell>;

impl SecondaryMesh {
    /// SecondaryMeshのインスタンスを作成
    /// primary_x:1次メッシュコードの下2桁
    /// primary_y:1次メッシュコードの上2桁
    /// x:2次メッシュコードの下1桁
    /// y:2次メッシュコードの上1桁
    /// TODO:順番が逆なのが気持ち悪いから修正。543870なら385407の順番で与える必要がある。気持ち悪すぎる。
    fn new(primary_lon_code: u8, primary_lat_code: u8, y: u8, x: u8, cells: CellComposite) -> Self {
        Self {
            primary_lon_code,
            primary_lat_code,
            secondary_lat_code: y,
            secondary_lon_code: x,
            xrain_cells: cells,
        }
    }

    /// TODO:記述
    fn assign_cells(&mut self, cell_composite: CellComposite) -> Result<()> {
        self.xrain_cells = cell_composite;
        Ok(())
    }

    /// csvファイルに保存する
    fn save_csv<P: AsRef<Path>>(&self, out_path: P) -> Result<()> {
        let mut wtr = Writer::from_path(out_path)?;
        let xsize: usize = 40;
        let ysize: usize = 40;

        for i in 0..xsize {
            let mut vline = Vec::<u16>::new();
            vline.reserve(ysize);
            for j in 0..ysize {
                let index = i * 40 + j;
                vline.push(self.xrain_cells.get(index).unwrap().strength);
            }
            wtr.serialize(vline)?;
        }
        wtr.flush()?;
        Ok(())
    }
}

/// Has quality and rainfall data.(cf. XRAIN document)
///
/// 雨量データと品質データ
///
#[repr(C)]
#[derive(Debug)]
pub struct XrainCell {
    ///品質データ
    quality: u16,
    ///雨量
    strength: u16,
}

fn take_streaming<C>(i: &[u8], c: C) -> IResult<&[u8], &[u8]>
where
    C: ToUsize,
{
    bytes::streaming::take(c)(i)
}

fn take_complete(i: &[u8]) -> IResult<&[u8], &[u8]> {
    bytes::complete::take(1u8)(i)
}

fn open_file<P: AsRef<Path>>(file_path: P) -> Result<Vec<u8>> {
    let mut file = std::fs::File::open(file_path).expect("file open failed");
    let mut buf: Vec<u8> = Vec::new();
    file.read_to_end(&mut buf).expect("file read failed");
    Ok(buf)
}

fn parse()->Result<BTreeMap<usize,Vec<SecondaryMesh>>>{
    todo!()
}

/// ヘッダーまで読み進めたスライスを返す（日本語正しいですか？)
fn read_header(bin_slice: &[u8]) -> Result<(&[u8], XrainHeader)> {
    let mut header = XrainHeader::default();

    let input = bin_slice;
    //固定値チェック:1byte
    println!("Checking 01");
    let (input, extracted) = take_streaming(input, 1u8).unwrap();
    assert_eq!(extracted, &[0xFD]);
    //地整識別チェック:1byte
    //TODO:チェックをどうするか。
    let (input, extracted) = take_streaming(input, 1u8).unwrap();
    header.owner = extracted[0];

    println!("Checking 02");
    //データ種別1:1byte
    let (input, extracted) = take_streaming(input, 1u8).unwrap();

    assert_eq!(extracted, &[0x80]);
    //データ種別2:1byte
    //これが0x01じゃなかったら合成レーダー雨量ではない。(じゃあそのデータはなんなのかは検証しない)
    let (input, extracted) = take_streaming(input, 1u8).unwrap();
    assert_eq!(extracted, &[0x01]);
    //データ種別3:2byte
    let (input, _extracted) = take_streaming(input, 2u8).unwrap();

    //ヘッダ種別:1byte
    let (input, extracted) = take_streaming(input, 1u8).unwrap();
    assert_eq!(extracted, &[0x01]);
    //観測値識別
    let (input, extracted) = take_streaming(input, 1u8).unwrap();
    assert_eq!(extracted, &[0x05]);
    //観測日時
    //TODO:Impl datetime
    let (input, _extracted) = take_streaming(input, 16u8).unwrap();

    //システムステータス
    let (input, _extracted) = take_streaming(input, 16u8).unwrap();

    //装置no.
    let (input, _extracted) = take_streaming(input, 1u8).unwrap();

    //11応答ステータス
    let (input, extracted) = take_streaming(input, 1u8).unwrap();
    header.response_status = extracted[0];

    //ブロック数
    println!("Checking block num");
    let (input, extracted) = take_streaming(input, 2u8).unwrap();
    let mut earr: [u8; 2] = [0; 2];
    (0..2).for_each(|i| {
        earr[i] = extracted[i];
        println!("{}", extracted[i]);
    });
    let block_num = u16::from_be_bytes(earr);
    println!("ブロック数 :{}", block_num);
    header.block_num = block_num;

    //データサイズ
    println!("Checking data size");

    let (input, extracted) = take_streaming(input, 4u8).unwrap();
    //TODO:earrじゃなくてもっとましな名前を付ける。
    let mut earr: [u8; 4] = [0; 4];
    (0..4).for_each(|i| {
        earr[i] = extracted[i];
        println!("{}", extracted[i]);
    });
    let datasize = u32::from_be_bytes(earr);
    header.data_size = datasize;
    println!("size :{}", datasize);
    //南西端の１次メッシュコード bottom_left
    let (input, extracted) = take_streaming(input, 2u8).unwrap();
    let byte_upper_mask: u8 = 0b11110000;
    let byte_lower_mask: u8 = 0b00001111;

    let lat_upper = (extracted[0] & byte_upper_mask) >> 4;
    let lat_lower = extracted[0] & byte_lower_mask;

    header.bottom_left_lat = lat_upper * 10 + lat_lower;

    let lon_upper = (extracted[1] & byte_upper_mask) >> 4;
    let lon_lower = extracted[1] & byte_lower_mask;

    header.bottom_left_lon = lon_upper * 10 + lon_lower;

    println!(
        "South-west primary mesh code :{}{}",
        header.bottom_left_lat, header.bottom_left_lon
    );

    //北東端の１次メッシュコード
    let (input, extracted) = take_streaming(input, 2u8).unwrap();

    let byte_upper_mask: u8 = 0b11110000;
    let byte_lower_mask: u8 = 0b00001111;
    let lat_upper = (extracted[0] & byte_upper_mask) >> 4;
    let lat_lower = extracted[0] & byte_lower_mask;

    header.top_right_lat = lat_upper * 10 + lat_lower;

    let lon_upper = (extracted[1] & byte_upper_mask) >> 4;
    let lon_lower = extracted[1] & byte_lower_mask;

    header.top_right_lon = lon_upper * 10 + lon_lower;

    println!(
        "North-east primary mesh code :{}{}",
        header.top_right_lat, header.top_right_lon
    );

    //予備領域をスキップ
    let (input, _extracted) = take_streaming(input, 10u8).unwrap();

    //固定値
    let (input, extracted) = take_streaming(input, 2u8).unwrap();
    assert_eq!(extracted, &[0x00, 0x00]);
    Ok((input, header))
}

/// ブロック内のすべてのセルを読み、Vec<SecondaryMesh>を返す。
fn read_sequential_block<'a>(input: &'a [u8]) -> Result<(&'a [u8], Vec<SecondaryMesh>)> {
    let (input_buf, block_header) = read_block_header(input)?;
    println!("{:?}", block_header);

    let block_len = block_header.length;

    let mut v_smesh: Vec<SecondaryMesh> = Vec::new();
    let mut i = 0;

    let mut buf = input_buf;
    //セル数だけ繰り返し
    while i < block_len {
        //先頭の2次メッシュコードに現在のセル番号を足して
        //現在の1次メッシュコードと2次メッシュコードを計算。

        //先頭の２次メッシュコードに処理しているブロックのインデックスを足す。
        //それを８で割るとどこの１次メッシュに属しているかがわかる。
        //TODO:u8で足りるよね？考える
        let currentx = block_header.first_x + i;
        let currenty = block_header.first_y;
        let primary_x = block_header.lon + (currentx / 8);
        let primary_y = block_header.lat;
        let currentx = currentx % 8;
        let (input_internal, cmp) = read_single_block(buf)?;
        buf = input_internal;
        let smesh = SecondaryMesh::new(primary_y, primary_x, currenty, currentx, cmp);
        v_smesh.push(smesh);
        i += 1;
    }

    Ok((buf, v_smesh))
}

/// ブロックの中のセルを一つ読む。
fn read_single_block(input: &[u8]) -> Result<(&[u8], CellComposite)> {
    let mut cellcmp = CellComposite::new();
    let mut buf = input;
    //一つのセルに入っているデータ数は40x40=1600
    for _i in 0..1600 {
        let (input_internal, new_cell) = read_cell(buf)?;
        buf = input_internal;
        cellcmp.push(new_cell);
    }
    Ok((buf, cellcmp))
}

/// 最小単位を読む。
/// FIXME:ブロックの中に含まれるものもセルと言うが、勝手にセルを東西南北に40分割したデータもセルと言っているまじでよくない。修正すべき。(DONE)
/// TODO:雨量データの観測範囲外とエラーデータの処理を書く。
fn read_cell(input: &[u8]) -> Result<(&[u8], XrainCell)> {
    //品質管理情報マスク
    let quality_mask: u16 = 0b1111000000000000;
    //雨量データマスク
    let rain_mask: u16 = 0b0000111111111111;
    let (out, extracted) = take_streaming(input, 2u8).unwrap();
    let mut cell_array: [u8; 2] = [0; 2];
    (0..2).for_each(|i| {
        cell_array[i] = extracted[i];
    });
    let val = u16::from_be_bytes(cell_array);
    let strength = val & rain_mask;
    let quality = val & quality_mask;
    let raincell = XrainCell { quality, strength };
    Ok((out, raincell))
}

/// ブロックヘッダーを読む
fn read_block_header(input: &[u8]) -> Result<(&[u8], XrainBlockHeader)> {
    //緯度
    let (input, lat) = take_streaming(input, 1u8).unwrap();
    //経度
    let (input, lon) = take_streaming(input, 1u8).unwrap();

    //let prim_mesh_code = Into::<u32>::into(lat[0]) * 100 + Into::<u32>::into(lon[0]);

    //１次メッシュコード上２桁
    let lat = lat[0];
    //１次メッシュコード下２桁
    let lon = lon[0];

    let (input, mesh_code) = take_streaming(input, 1u8).unwrap();

    let grid_position: u8 = mesh_code[0];
    let ymask: u8 = 0b11110000;
    let xmask: u8 = 0b00001111;

    //西北、南北方向にそれぞれ８分割した位置
    //１次メッシュ内での経度位置(西から東,)
    let xnum = grid_position & xmask;
    //１次メッシュ内での緯度位置(南から北,)
    let ynum = (grid_position & ymask) >> 4;

    //連続するブロック数
    let (input, block_num) = take_streaming(input, 1u8).unwrap();

    let block_header = XrainBlockHeader {
        lat,
        lon,
        first_x: xnum,
        first_y: ynum,
        length: block_num[0],
    };

    Ok((input, block_header))
}

/// XRAIN dataset
/// It contains header and data.
/// In future, it can be handled with gdal.
#[repr(C)]
pub struct CXrainDataset {
    ///TODO:CXrainDatasetをmem::forgetした後、
    /// XRAINheaderはどうなるんだろう。Dropされるのかな？
    header: XrainHeader,

    ///配列のポインタ。
    ptr: *mut XrainCell,
    ///The number of XrainCell.
    length: u64,
}

#[repr(C)]
pub struct CXrainResult {
    status: bool,
    data: CXrainDataset,
}

fn open_internal<P: AsRef<Path>>(file_path: P) -> Result<CXrainDataset> {
    let xrain = open_file(file_path)?;
    let (input, header) = read_header(xrain.as_slice())?;

    let mut buf = input;

    let mut i: u16 = 0;
    while i < header.block_num {
        let (input_internal, meshes) = read_sequential_block(buf)?;
        if meshes.is_empty() {
            return Err(anyhow::anyhow!(
                "Mesh vector is empty! Some failure occured."
            ));
        }

        //Start code

        //End code

        //Finalize
        //以下触るな
        buf = input_internal;
        i += 1;
    }

    todo!()
}

/// Open and get XRAIN dataset.
#[no_mangle]
pub extern "C" fn open(file_path: *const c_char) -> Option<CXrainResult> {
    let c_strpath = unsafe { CStr::from_ptr(file_path) };
    let path = c_strpath.to_str();
    if let std::result::Result::Ok(p) = path {
        let xrain = open_file(p);
        if let std::result::Result::Ok(data) = xrain {
            let res = read_header(data.as_slice());
            if let std::result::Result::Ok((input, header)) = res {}
        } else {
            return None;
        }
        return None;
    } else {
        return None;
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_header_read() -> Result<()> {
        let data = open_file("KANTO00001-20191011-0000-G000-EL000000")?;
        let (input, header) = read_header(data.as_slice())?;
        assert_eq!(header.bottom_left_lat,46);
        assert_eq!(header.bottom_left_lon,34);
        assert_eq!(header.top_right_lat,55);
        assert_eq!(header.top_right_lon,43);
        Ok(())
    }

    #[test]
    fn test_read_single_block() -> Result<()> {
        let data = open_file("KANTO00001-20191011-0200-G000-EL000000")?;
        let (input, header) = read_header(data.as_slice())?;

        let mut buf = input;

        let mut i: u16 = 0;
        while i < header.block_num {
            let (input_internal, meshes) = read_sequential_block(buf)?;
            let mut tmeshes: Vec<SecondaryMesh> = meshes
                .into_iter()
                .filter(|f| f.primary_lat_code == 54 && f.primary_lon_code == 38)
                .collect();
            if tmeshes.is_empty() {
            } else {
                tmeshes.sort_by(|lhs, rhs| {
                    return lhs.secondary_lon_code.cmp(&rhs.secondary_lon_code);
                });

                for v in tmeshes.into_iter() {
                    println!(
                        "{}{}{}{}",
                        v.primary_lat_code,
                        v.primary_lon_code,
                        v.secondary_lat_code,
                        v.secondary_lon_code
                    );
                    let name = v.primary_lat_code.to_string();
                    let name = name
                        + v.primary_lon_code.to_string().as_str()
                        + v.secondary_lat_code.to_string().as_str()
                        + v.secondary_lon_code.to_string().as_str()
                        + ".csv";

                    let mut out_path = PathBuf::from("data");
                    out_path.push(name);

                    v.save_csv(out_path)?;
                }
            }
            // for v in meshes.iter() {
            //     let file_name = v.primary_y.to_string() + v.primary_x.to_string().as_str();

            //     let file_name =
            //         file_name + v.y.to_string().as_str() + v.x.to_string().as_str() + ".csv";

            //     let mut file_path = PathBuf::from("out");
            //     file_path.push(file_name);

            //     v.save_csv(file_path)?;
            // }

            buf = input_internal;

            i += 1;
        }

        Ok(())
    }

    #[test]
    fn tedst_while() -> Result<()> {
        let mut idx: usize = 0;
        while idx < 10 {
            println!("{}", idx);
            idx += 1;
        }
        for i in 0..10 {}
        Ok(())
    }
}
