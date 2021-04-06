extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn;

#[proc_macro_derive(AppData)]
pub fn app_data_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_app_data_macro(&ast)
}

fn impl_app_data_macro(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let gen = quote! {
        impl AppData for #name {}
    };
    gen.into()
}

#[proc_macro_derive(ProctorContext)]
pub fn proctor_context_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_proctor_context_macro(&ast)
}

fn impl_proctor_context_macro(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let gen = quote! {
        impl ProctorContext for #name {}
    };
    gen.into()
}